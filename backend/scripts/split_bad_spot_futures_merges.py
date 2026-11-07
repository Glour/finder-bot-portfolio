#!/usr/bin/env python3
"""
Находит и разделяет SPOT+FUTURES пары с аномальным спредом (>20%).
Добавляет их в global_token_pair_blacklist.
"""
import asyncio
import sys
sys.path.insert(0, '/app')

from sqlalchemy import text, select
from infrastructure.database import setup as db_setup
from infrastructure.database.requests import get_database_repo
from infrastructure.database.models.tokens import GlobalToken, GlobalTokenPairBlacklist
from settings import settings
from uuid import uuid4
from datetime import datetime, timezone


async def find_bad_spot_futures_pairs():
    """Находит GT где SPOT и FUTURES имеют spread >20% по сигналам за 24h"""
    repo = get_database_repo()
    async with repo.sessionmaker() as session:
        # Находим пары из signal_history где SPOT vs FUTURES на одной бирже
        query = text("""
            WITH bad_signals AS (
                -- Ищем сигналы где SPOT<->FUTURES с одного exchange
                SELECT DISTINCT
                    sh.symbol,
                    sh.spread_percent,
                    sh.buy_from,
                    sh.sell_to,
                    CASE 
                        WHEN sh.buy_from ~* '^spot_' AND sh.sell_to ~* '^futures_' 
                        THEN SUBSTRING(sh.buy_from FROM '^spot_(.*)$')
                        WHEN sh.buy_from ~* '^futures_' AND sh.sell_to ~* '^spot_'
                        THEN SUBSTRING(sh.buy_from FROM '^futures_(.*)$')
                    END as exchange
                FROM signal_history sh
                WHERE sh.created_at > now() - interval '24 hours'
                  AND sh.spread_percent > 2000  -- >2000% явно артефакт
                  AND (
                      (sh.buy_from ~* '^spot_' AND sh.sell_to ~* '^futures_' 
                       AND SUBSTRING(sh.buy_from FROM '^spot_(.*)$') = SUBSTRING(sh.sell_to FROM '^futures_(.*)$'))
                      OR
                      (sh.buy_from ~* '^futures_' AND sh.sell_to ~* '^spot_'
                       AND SUBSTRING(sh.buy_from FROM '^futures_(.*)$') = SUBSTRING(sh.sell_to FROM '^spot_(.*)$'))
                  )
            ),
            -- Группируем по symbol+exchange
            bad_pairs AS (
                SELECT 
                    symbol,
                    exchange,
                    MAX(spread_percent) as max_spread,
                    COUNT(*) as signal_count
                FROM bad_signals
                GROUP BY symbol, exchange
                HAVING COUNT(*) > 2  -- Минимум 3 сигнала
            ),
            -- Находим GT для этих пар
            gt_matches AS (
                SELECT DISTINCT
                    bp.symbol as signal_symbol,
                    bp.exchange,
                    bp.max_spread,
                    bp.signal_count,
                    gt.id as global_token_id,
                    gt.symbol as gt_symbol,
                    t.base_asset
                FROM bad_pairs bp
                JOIN tokens t ON UPPER(t.base_asset) = UPPER(bp.symbol)
                JOIN sources s ON s.id = t.source_id AND s.name = bp.exchange
                JOIN token_chains tc ON tc.token_id = t.id
                JOIN global_tokens gt ON gt.id = tc.global_token_id
                WHERE gt.is_deleted = false
            ),
            -- Проверяем что GT действительно содержит SPOT и FUTURES
            final_check AS (
                SELECT 
                    gm.signal_symbol,
                    gm.exchange,
                    gm.max_spread,
                    gm.signal_count,
                    gm.global_token_id,
                    gm.gt_symbol,
                    gm.base_asset,
                    COUNT(DISTINCT CASE WHEN t.market_type = 'SPOT' THEN t.id END) as spot_count,
                    COUNT(DISTINCT CASE WHEN t.market_type = 'FUTURES' THEN t.id END) as futures_count
                FROM gt_matches gm
                JOIN token_chains tc ON tc.global_token_id = gm.global_token_id
                JOIN tokens t ON t.id = tc.token_id
                JOIN sources s ON s.id = t.source_id
                WHERE t.base_asset = gm.base_asset
                  AND s.name = gm.exchange
                GROUP BY gm.signal_symbol, gm.exchange, gm.max_spread, gm.signal_count, 
                         gm.global_token_id, gm.gt_symbol, gm.base_asset
                HAVING COUNT(DISTINCT t.market_type) > 1
            )
            SELECT DISTINCT
                global_token_id,
                gt_symbol,
                base_asset,
                exchange,
                max_spread,
                signal_count,
                spot_count,
                futures_count
            FROM final_check
            ORDER BY max_spread DESC
        """)
        
        result = await session.execute(query)
        rows = result.fetchall()
        
        print(f"\n🔍 Найдено {len(rows)} проблемных GT с аномальным SPOT+FUTURES merge:\n")
        
        for row in rows:
            print(f"  {row.gt_symbol:15} | {row.base_asset:10} @ {row.exchange:10} | "
                  f"Spread: {row.max_spread:>7.0f}% | Signals: {row.signal_count:>3} | "
                  f"SPOT: {row.spot_count} FUTURES: {row.futures_count}")
        
        return rows


async def split_and_blacklist(gt_id: str, gt_symbol: str, base_asset: str, exchange: str):
    """Разделяет SPOT и FUTURES для GT и добавляет в blacklist"""
    repo = get_database_repo()
    async with repo.sessionmaker() as session:
        # 1. Находим SPOT и FUTURES токены
        query = text("""
            SELECT 
                t.id as token_id,
                t.market_type,
                tc.id as token_chain_id,
                tc.global_token_id
            FROM token_chains tc
            JOIN tokens t ON t.id = tc.token_id
            JOIN sources s ON s.id = t.source_id
            WHERE tc.global_token_id = :gt_id
              AND t.base_asset = :base_asset
              AND s.name = :exchange
            ORDER BY t.market_type
        """)
        
        result = await session.execute(
            query, 
            {"gt_id": gt_id, "base_asset": base_asset, "exchange": exchange}
        )
        tokens = result.fetchall()
        
        if len(tokens) < 2:
            print(f"    ⚠️  Недостаточно токенов для {gt_symbol}")
            return False
        
        spot_token = None
        futures_token = None
        
        for t in tokens:
            if t.market_type.name == 'SPOT':
                spot_token = t
            elif t.market_type.name == 'FUTURES':
                futures_token = t
        
        if not spot_token or not futures_token:
            print(f"    ⚠️  Не найдены SPOT и FUTURES для {gt_symbol}")
            return False
        
        # 2. Создаём новый GT для FUTURES
        new_gt_symbol = f"{base_asset}_F_SPLIT"
        
        # Проверяем существование
        existing = await session.execute(
            select(GlobalToken).where(
                GlobalToken.symbol == new_gt_symbol,
                GlobalToken.is_deleted == False
            )
        )
        existing_gt = existing.scalar_one_or_none()
        
        if existing_gt:
            new_gt_id = existing_gt.id
            print(f"    ℹ️  Используем существующий GT: {new_gt_symbol}")
        else:
            new_gt_id = uuid4()
            new_gt = GlobalToken(
                id=new_gt_id,
                symbol=new_gt_symbol,
                is_deleted=False,
                created_at=datetime.now(timezone.utc),
                updated_at=datetime.now(timezone.utc)
            )
            session.add(new_gt)
            print(f"    ✅ Создан новый GT: {new_gt_symbol}")
        
        # 3. Перемещаем FUTURES token_chain на новый GT
        await session.execute(
            text("""
                UPDATE token_chains
                SET global_token_id = :new_gt_id,
                    updated_at = now()
                WHERE id = :token_chain_id
            """),
            {"new_gt_id": new_gt_id, "token_chain_id": futures_token.token_chain_id}
        )
        
        # 4. Копируем sources для нового GT
        await session.execute(
            text("""
                INSERT INTO global_token_sources (id, global_token_id, source_id, created_at, updated_at)
                SELECT gen_random_uuid(), :new_gt_id, source_id, now(), now()
                FROM global_token_sources
                WHERE global_token_id = :old_gt_id
                ON CONFLICT DO NOTHING
            """),
            {"new_gt_id": new_gt_id, "old_gt_id": gt_id}
        )
        
        # 5. Перемещаем цены FUTURES на новый GT
        await session.execute(
            text("""
                UPDATE token_prices
                SET global_token_id = :new_gt_id,
                    updated_at = now()
                WHERE token_id = :futures_token_id
            """),
            {"new_gt_id": new_gt_id, "futures_token_id": futures_token.token_id}
        )
        
        # 6. Добавляем в blacklist
        blacklist_entry = GlobalTokenPairBlacklist(
            id=uuid4(),
            global_token_id_1=gt_id,
            global_token_id_2=new_gt_id,
            reason=f"Auto-split: SPOT vs FUTURES anomaly on {exchange}",
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc)
        )
        session.add(blacklist_entry)
        
        await session.commit()
        
        print(f"    ✅ Разделено: {gt_symbol} (SPOT) + {new_gt_symbol} (FUTURES)")
        print(f"    ✅ Добавлено в blacklist")
        return True


async def main():
    # Initialize database
    db_setup.init_database(settings)
    
    print("=" * 70)
    print("🔧 SPLIT BAD SPOT+FUTURES MERGES")
    print("=" * 70)
    
    # 1. Находим проблемные пары
    bad_pairs = await find_bad_spot_futures_pairs()
    
    if not bad_pairs:
        print("\n✅ Проблемных merge не найдено!")
        return
    
    # 2. Спрашиваем подтверждение
    print(f"\n⚠️  Будет разделено {len(bad_pairs)} пар.")
    confirm = input("Продолжить? (y/N): ").strip().lower()
    
    if confirm != 'y':
        print("❌ Отменено")
        return
    
    # 3. Разделяем и добавляем в blacklist
    print("\n🔨 Начинаю разделение...\n")
    
    success_count = 0
    for row in bad_pairs:
        print(f"Обрабатываю: {row.gt_symbol} ({row.base_asset} @ {row.exchange})...")
        
        if await split_and_blacklist(
            str(row.global_token_id),
            row.gt_symbol,
            row.base_asset,
            row.exchange
        ):
            success_count += 1
        
        print()
    
    print("=" * 70)
    print(f"✅ Завершено! Разделено: {success_count}/{len(bad_pairs)}")
    print("=" * 70)
    print("\n⚠️  Перезапустите spread collectors:")
    print("    docker restart $(docker ps --filter 'name=spread' --format '{{.Names}}')")
    print("    docker exec -i redis redis-cli FLUSHALL")


if __name__ == "__main__":
    asyncio.run(main())
