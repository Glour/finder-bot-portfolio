from datetime import datetime, UTC
from uuid import UUID

import orjson
from sqlalchemy import select, func, text, delete, or_, update, and_, exists
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.orm import selectinload

from infrastructure.database.models import Token, TokenOnChain, GlobalTokenSource
from infrastructure.database.models.base import MarketType, MergeStatusType
from infrastructure.database.models.tokens import TokenPrice, DexPair, GlobalToken, TokenBlacklist, GlobalMergeCandidate, GlobalTokenPairBlacklist
from infrastructure.database.repo.base import BaseRepo


class GlobalTokenRepo(BaseRepo):
    def __init__(self, session: async_sessionmaker):
        super().__init__(session, GlobalToken)  # noqa

    async def get_by_symbol(self, symbol: str) -> GlobalToken | None:
        async with self.sessionmaker() as session:
            stmt = select(GlobalToken).filter_by(symbol=symbol, is_deleted=False).limit(1)
            result = await session.execute(stmt)
            return result.scalars().first()

    async def get_by_symbols(self, symbols: list[str]) -> list[GlobalToken]:
        """Get global tokens by list of symbols."""
        if not symbols:
            return []
        async with self.sessionmaker() as session:
            stmt = select(GlobalToken).where(
                GlobalToken.symbol.in_(symbols),
                GlobalToken.is_deleted == False  # noqa: E712
            )
            result = await session.execute(stmt)
            return list(result.scalars().all())

    async def get_by_symbols_with_meta(self, symbols: list[str]) -> list[GlobalToken]:
        """Get global tokens by symbols with sources and chain info loaded."""
        if not symbols:
            return []
        async with self.sessionmaker() as session:
            stmt = (
                select(GlobalToken)
                .where(
                    GlobalToken.symbol.in_(symbols),
                    GlobalToken.is_deleted == False  # noqa: E712
                )
                .options(
                    selectinload(GlobalToken.sources),
                    selectinload(GlobalToken.token_on_chains).selectinload(TokenOnChain.chain),
                    selectinload(GlobalToken.token_on_chains).selectinload(TokenOnChain.token),
                )
            )
            result = await session.execute(stmt)
            return list(result.scalars().all())

    async def get_by_symbols_with_prices(self, symbols: list[str]) -> list[GlobalToken]:
        """Get global tokens by list of symbols with prices loaded."""
        if not symbols:
            return []
        async with self.sessionmaker() as session:
            stmt = (
                select(GlobalToken)
                .options(selectinload(GlobalToken.prices))
                .where(
                    GlobalToken.symbol.in_(symbols),
                    GlobalToken.is_deleted == False  # noqa: E712
                )
            )
            result = await session.execute(stmt)
            return list(result.scalars().all())

    async def bulk_create(self, data: list[dict]) -> list[GlobalToken]:
        if not data:
            return []

        async with self.sessionmaker() as session:
            symbols = [d["symbol"] for d in data]
            
            # CRITICAL FIX (2026-02-19): Use ON CONFLICT DO NOTHING to handle race conditions
            # Multiple token-collectors can try to create same GlobalToken simultaneously
            stmt = (
                insert(GlobalToken)
                .values(data)
                .on_conflict_do_nothing(
                    index_elements=["symbol"],
                    index_where=(GlobalToken.is_deleted == False)
                )
                .returning(GlobalToken)
            )
            
            result = await session.execute(stmt)
            await session.commit()
            created_tokens = list(result.scalars().all())
            
            # Get all tokens (created + existing) to return complete list
            all_stmt = select(GlobalToken).where(
                GlobalToken.symbol.in_(symbols),
                GlobalToken.is_deleted == False  # noqa: E712
            )
            all_result = await session.execute(all_stmt)
            all_tokens = list(all_result.scalars().all())
            
            return all_tokens

    async def get_all_with_meta(self):
        async with self.sessionmaker() as session:
            stmt = (
                select(GlobalToken)
                .options(
                    selectinload(GlobalToken.sources),
                    selectinload(GlobalToken.token_on_chains).selectinload(TokenOnChain.chain),
                    selectinload(GlobalToken.token_on_chains).selectinload(TokenOnChain.token),
                    selectinload(GlobalToken.prices),
                )
            )
            result = await session.execute(stmt)
            return list(result.scalars().all())


    async def get_by_chain_address(self, chain_name: str, contract_address: str) -> GlobalToken | None:
        """Find GlobalToken by chain name and contract address (case-insensitive address match)."""
        from infrastructure.database.models.chains import Chain
        async with self.sessionmaker() as session:
            stmt = (
                select(GlobalToken)
                .join(TokenOnChain, TokenOnChain.global_token_id == GlobalToken.id)
                .join(Chain, Chain.id == TokenOnChain.chain_id)
                .where(
                    Chain.name == chain_name,
                    func.lower(TokenOnChain.contract_address) == contract_address.lower(),
                    GlobalToken.is_deleted == False  # noqa: E712
                )
                .limit(1)
            )
            result = await session.execute(stmt)
            return result.scalars().first()

    async def get_by_id_with_meta(self, global_token_id):
        """Get global_token by ID with all relationships loaded."""
        async with self.sessionmaker() as session:
            stmt = (
                select(GlobalToken)
                .where(GlobalToken.id == global_token_id)
                .options(
                    selectinload(GlobalToken.sources),
                    selectinload(GlobalToken.token_on_chains).selectinload(TokenOnChain.chain),
                    selectinload(GlobalToken.token_on_chains).selectinload(TokenOnChain.token),
                    selectinload(GlobalToken.prices),
                )
            )
            result = await session.execute(stmt)
            return result.scalar_one_or_none()

    async def get_all_with_data(
            self,
            limit: int | None = None,
            offset: int | None = None,
    ) -> list[dict]:
        lim = f"LIMIT {int(limit)}" if limit else ""
        off = f"OFFSET {int(offset)}" if offset else ""

        # 1. Получаем глобальные токены без фильтра blacklist
        query_gt = f"""
        SELECT g.id, g.symbol
        FROM global_tokens g
        WHERE g.is_deleted = false
          AND EXISTS (
              SELECT 1
              FROM token_chains tc
              JOIN tokens t ON t.id = tc.token_id
              WHERE tc.global_token_id = g.id
          )
        ORDER BY g.id DESC
        {lim}
        {off}
        """
        async with self.sessionmaker() as session:
            res = await session.execute(text(query_gt))
            gt_rows = res.mappings().all()

        if not gt_rows:
            return []

        gt_ids = [r["id"] for r in gt_rows]
        params = {"gt_ids": tuple(gt_ids)}

        # 2. Получаем все цены и токены
        query_prices = """
        SELECT
          tp.global_token_id,
          tp.global_token_source_id,
          tp.market_type,
          tp.updated_at,
          tp.created_at,
          tp.token_id,
          t.contract_size,
          t.base_asset,
          t.quote_asset,
          t.symbol AS token_symbol,
          gts.id as gts_id,
          s.id as source_id,
          s.name as source_name,
          s.type as source_type
        FROM token_prices tp
        JOIN tokens t ON t.id = tp.token_id
        JOIN global_token_sources gts ON gts.id = tp.global_token_source_id
        JOIN sources s ON s.id = gts.source_id
        WHERE tp.global_token_id = ANY(:gt_ids)
        ORDER BY tp.updated_at DESC
        """
        async with self.sessionmaker() as session:
            res = await session.execute(text(query_prices), params)
            price_rows = res.mappings().all()

        # 3. Получаем токены на цепочках
        token_ids = list({r["token_id"] for r in price_rows})
        chains_by_token = {}
        if token_ids:
            params_chains = {"token_ids": tuple(token_ids)}
            query_chains = """
            SELECT
              toc.token_id,
              c.name AS chain_name,
              toc.deposit_enabled,
              toc.withdraw_enabled,
              toc.contract_address,
              toc.deposit_tax,
              toc.withdrawal_fee,
              toc.withdraw_tax,
              toc.deposit_enabled_changed_at,
              toc.withdraw_enabled_changed_at
            FROM token_chains toc
            JOIN chains c ON c.id = toc.chain_id
            WHERE toc.token_id = ANY(:token_ids)
            """
            async with self.sessionmaker() as session:
                res = await session.execute(text(query_chains), params_chains)
                chain_rows = res.mappings().all()
            for cr in chain_rows:
                chains_by_token.setdefault(cr["token_id"], []).append({
                    "chain_name": cr["chain_name"],
                    "deposit_enabled": cr["deposit_enabled"],
                    "withdraw_enabled": cr["withdraw_enabled"],
                    "contract_address": cr["contract_address"],
                    "deposit_tax": cr["deposit_tax"],
                    "withdrawal_fee": cr["withdrawal_fee"],
                    "withdraw_tax": cr["withdraw_tax"],
                    "deposit_enabled_changed_at": cr["deposit_enabled_changed_at"],
                    "withdraw_enabled_changed_at": cr["withdraw_enabled_changed_at"],
                })

        # 4. Группировка цен по глобальному токену
        prices_by_gt = {}
        for r in price_rows:
            gt = r["global_token_id"]
            p = {
                "market_type": r["market_type"],
                "updated_at": r["updated_at"],
                "created_at": r["created_at"],
                "token": {
                    "id": r["token_id"],
                    "contract_size": r["contract_size"],
                    "base_asset": r["base_asset"],
                    "quote_asset": r["quote_asset"],
                    "symbol": r["token_symbol"],
                },
                "source": {
                    "id": r["source_id"],
                    "name": r["source_name"],
                    "type": r["source_type"],
                },
                "global_token_source_id": r["global_token_source_id"],
                "chains": chains_by_token.get(r["token_id"], []),
            }
            prices_by_gt.setdefault(gt, []).append(p)

        # 5. Финальная сборка
        out = []
        for r in gt_rows:
            gt_id = r["id"]
            out.append({
                "id": gt_id,
                "symbol": r["symbol"],
                "prices": prices_by_gt.get(gt_id, []),
            })

        return out


class TokenRepo(BaseRepo):
    def __init__(self, session: async_sessionmaker):
        super().__init__(session, Token)  # noqa

    async def get_all(
            self,
            limit: int | None = None,
            offset: int | None = None,
            source_id: UUID | None = None,
            market_type: MarketType | None = None
    ) -> list[Token]:
        async with self.sessionmaker() as session:
            stmt = (
                select(Token)
                .order_by(Token.id.desc())
                .options(
                    selectinload(Token.token_on_chains),
                )
            )

            if source_id:
                stmt = stmt.where(Token.source_id == source_id)
            if market_type:
                stmt = stmt.where(Token.market_type == market_type)
            if limit:
                stmt = stmt.limit(limit)
            if offset:
                stmt = stmt.offset(offset)

            result = await session.execute(stmt)
            return list(result.scalars().all())

    async def get_token_id_map(self, source_id: UUID) -> dict[tuple[str, MarketType], UUID]:
        """Lightweight method to get token ID mapping without loading full objects"""
        async with self.sessionmaker() as session:
            stmt = select(Token.id, Token.symbol, Token.market_type).where(Token.source_id == source_id)
            result = await session.execute(stmt)
            return {(row.symbol, row.market_type): row.id for row in result}

    async def get_with_data(self, obj_id) -> list[Token]:
        async with self.sessionmaker() as session:
            stmt = (
                select(Token)
                .filter_by(id=obj_id)
                .options(
                    selectinload(Token.token_on_chains)
                )
            )
            result = await session.execute(stmt)
            return list(result.scalars().all())

    async def get_by_global_token_and_source(
        self,
        global_token_id: UUID,
        source_id: UUID
    ) -> list[Token]:
        """Get all tokens for a specific global_token from a specific source"""
        from infrastructure.database.models.tokens import TokenOnChain as TokenChain
        async with self.sessionmaker() as session:
            stmt = (
                select(Token)
                .distinct()
                .join(TokenChain, TokenChain.token_id == Token.id)
                .where(
                    TokenChain.global_token_id == global_token_id,
                    Token.source_id == source_id
                )
            )
            result = await session.execute(stmt)
            return list(result.scalars().all())

    async def bulk_create_or_update(self, tokens_data: list[dict]) -> list[Token]:
        if not tokens_data:
            return []

        extra_fields = ["chains", "base_coin_name", "price_usd", "liquidity_usd"]
        clean_tokens = [{k: v for k, v in token.items() if k not in extra_fields} for token in tokens_data]

        seen = {}
        for token in clean_tokens:
            key = (token["symbol"], token["market_type"])
            existing = seen.get(key)
            if existing:
                if not existing.get("contract_size") and token.get("contract_size"):
                    existing["contract_size"] = token["contract_size"]
                continue

            seen[key] = token

        unique_tokens = list(seen.values())

        stmt = (
            insert(Token)
            .values(unique_tokens)
            .on_conflict_do_update(
                index_elements=["symbol", "market_type", "source_id"],
                set_={
                    "base_asset": insert(Token).excluded.base_asset,
                    "quote_asset": insert(Token).excluded.quote_asset,
                    "contract_size": insert(Token).excluded.contract_size,
                    "is_active": insert(Token).excluded.is_active,
                    "updated_at": insert(Token).excluded.updated_at,
                }
            )
            .returning(Token)
        )

        async with self.sessionmaker() as session:
            result = await session.execute(stmt)
            await session.commit()
            return list(result.scalars().all())

    async def count_tokens(self, source_id: UUID, market_type: MarketType) -> int:
        async with self.sessionmaker() as session:
            stmt = (
                select(func.count())
                .select_from(Token)
                .filter_by(source_id=source_id, market_type=market_type)
            )
            result = await session.execute(stmt)
            return result.scalar()

    async def exists(self, symbol, market_type, source_id) -> bool:
        async with self.sessionmaker() as session:
            stmt = (
                select(Token)
                .filter_by(symbol=symbol, market_type=market_type, source_id=source_id)
            )
            result = await session.execute(stmt)
            return bool(result.scalar_one_or_none())


class TokenPriceRepo(BaseRepo):
    def __init__(self, session: async_sessionmaker):
        super().__init__(session, TokenPrice)  # noqa

    async def bulk_upsert(self, prices: list[dict]):
        if not prices:
            return []

        for row in prices:
            if 'orderbook' in row and isinstance(row['orderbook'], dict):
                row['orderbook'] = orjson.dumps(row['orderbook']).decode('utf-8')

        query = text("""
        INSERT INTO token_prices (
            global_token_id,
            global_token_source_id,
            token_id,
            orderbook,
            market_type,
            price_usd,
            created_at,
            updated_at
        )
        VALUES (:global_token_id, :global_token_source_id, :token_id, :orderbook, :market_type, :price_usd, NOW(), NOW())
        ON CONFLICT (global_token_id, global_token_source_id, market_type)
        DO UPDATE SET
            orderbook = EXCLUDED.orderbook,
            price_usd = EXCLUDED.price_usd,
            updated_at = NOW();
        """)

        async with self.sessionmaker() as session:
            await session.execute(query, prices)
            await session.commit()
            return None


class TokenBlacklistRepo(BaseRepo):
    def __init__(self, session: async_sessionmaker):
        super().__init__(session, TokenBlacklist)  # noqa

    async def get_by_chain_and_contract(self, chain_name: str, contract_address: str) -> TokenBlacklist | None:
        async with self.sessionmaker() as session:
            stmt = (
                select(TokenBlacklist)
                .filter_by(chain_name=chain_name, contract_address=contract_address)
            )
            result = await session.execute(stmt)
            return result.scalar_one_or_none()

    async def delete_by_chain_and_contract(self, chain_name: str, contract_address: str):
        async with self.sessionmaker() as session:
            stmt = (
                delete(TokenBlacklist)
                .filter_by(chain_name=chain_name, contract_address=contract_address)
            )
            await session.execute(stmt)
            await session.commit()

    async def get_or_create(
        self,
        chain_name=None,
        contract_address=None,
        global_token_id=None,
        mute_type=None,
        expires_at=None
    ) -> TokenBlacklist | None:
        async with self.sessionmaker() as session:
            # Check if already exists with proper uniqueness logic:
            # 1. If global_token_id + no chain_name: global mute (unique by global_token_id, chain_name IS NULL)
            # 2. If global_token_id + chain_name: token mute in specific chain (unique by global_token_id + chain_name)
            # 3. If no global_token_id: legacy chain+address mute (unique by chain_name + contract_address)

            if global_token_id:
                if chain_name:
                    # Token in specific chain
                    stmt = select(TokenBlacklist).filter_by(
                        global_token_id=global_token_id,
                        chain_name=chain_name
                    )
                else:
                    # Global token mute (all chains)
                    stmt = select(TokenBlacklist).filter(
                        TokenBlacklist.global_token_id == global_token_id,
                        TokenBlacklist.chain_name.is_(None)
                    )
            else:
                # Legacy: chain + address
                stmt = select(TokenBlacklist).filter_by(
                    chain_name=chain_name,
                    contract_address=contract_address
                )

            result = await session.execute(stmt)
            exists_token = result.scalar_one_or_none()

            if exists_token:
                return exists_token

            insert_stmt = (
                insert(TokenBlacklist)
                .values(
                    chain_name=chain_name,
                    contract_address=contract_address,
                    global_token_id=global_token_id,
                    mute_type=mute_type,
                    expires_at=expires_at
                )
                .returning(TokenBlacklist)
            )

            result = await session.execute(insert_stmt)
            await session.commit()
            return result.scalar_one_or_none()

    async def get_all(self, limit: int | None = None, offset: int | None = None):
        async with self.sessionmaker() as session:
            now = datetime.now(UTC).replace(tzinfo=None)
            stmt = (
                select(TokenBlacklist)
                .options(selectinload(TokenBlacklist.global_token))
                .where(
                    or_(
                        TokenBlacklist.expires_at.is_(None),
                        TokenBlacklist.expires_at > now
                    ))
            )
            result = await session.execute(stmt)
            return list(result.scalars().all())


class DexPairRepo(BaseRepo):
    def __init__(self, session: async_sessionmaker):
        super().__init__(session, DexPair)  # noqa

    async def bulk_upsert_pairs(self, data: list[dict]):
        seen_keys = set()
        unique_rows = []
        if not data:
            return

        for row in data:
            key = (row["pair_address"], row["source_id"], row["chain_id"])
            if key not in seen_keys:
                seen_keys.add(key)
                unique_rows.append(row)

        stmt = (
            insert(DexPair)
            .values(unique_rows)
        )
        update_fields = {
            "url": stmt.excluded.url,
            "updated_at": func.now()
        }

        stmt = stmt.on_conflict_do_update(
            index_elements=["chain_id", "pair_address"],
            set_=update_fields
        )

        async with self.sessionmaker() as session:
            await session.execute(stmt)
            await session.commit()


class GlobalMergeCandidateRepo(BaseRepo):
    def __init__(self, session: async_sessionmaker):
        super().__init__(session, GlobalMergeCandidate)  # noqa

    async def bulk_insert(self, records: list[dict]) -> list[GlobalMergeCandidate] | None:
        if not records:
            return None

        async with self.sessionmaker() as session:
            stmt = (
                insert(GlobalMergeCandidate)
                .values(records)
                .on_conflict_do_nothing(
                    index_elements=["global_token_a_id", "global_token_b_id"],
                )
                .returning(GlobalMergeCandidate.id)
            )

            result = await session.execute(stmt)
            inserted_ids = [row[0] for row in result.fetchall()]
            await session.commit()

            if not inserted_ids:
                return None

            stmt = (
                select(GlobalMergeCandidate)
                .options(
                    selectinload(GlobalMergeCandidate.global_token_a)
                    .selectinload(GlobalToken.token_on_chains)
                    .selectinload(TokenOnChain.token)
                    .selectinload(Token.source),
                    selectinload(GlobalMergeCandidate.global_token_a)
                    .selectinload(GlobalToken.token_on_chains)
                    .selectinload(TokenOnChain.chain),
                    selectinload(GlobalMergeCandidate.global_token_b)
                    .selectinload(GlobalToken.token_on_chains)
                    .selectinload(TokenOnChain.token)
                    .selectinload(Token.source),
                    selectinload(GlobalMergeCandidate.global_token_b)
                    .selectinload(GlobalToken.token_on_chains)
                    .selectinload(TokenOnChain.chain),
                )
                .where(GlobalMergeCandidate.id.in_(inserted_ids))
            )

            result = await session.execute(stmt)
            return list(result.scalars().all())

    async def approve_candidate(self, global_token_a_id: UUID, global_token_b_id: UUID):
        async with self.sessionmaker() as session:
            result_a = await session.execute(
                select(GlobalToken)
                .options(selectinload(GlobalToken.token_on_chains))
                .where(GlobalToken.id == global_token_a_id)
            )
            a = result_a.scalars().first()

            result_b = await session.execute(
                select(GlobalToken)
                .options(selectinload(GlobalToken.token_on_chains))
                .where(GlobalToken.id == global_token_b_id)
            )
            b = result_b.scalars().first()

            if not a or not b:
                raise ValueError(f"GlobalToken not found: a={global_token_a_id}, b={global_token_b_id}")

            if b.token_on_chains:
                token_ids_b = [t.id for t in b.token_on_chains if t.id is not None]
                if token_ids_b:
                    await session.execute(
                        update(TokenOnChain)
                        .where(TokenOnChain.id.in_(token_ids_b))
                        .values(global_token_id=global_token_a_id)
                    )

            await session.execute(
                delete(TokenPrice)
                .where(TokenPrice.global_token_id == global_token_b_id)
                .where(
                    exists().where(
                        (TokenPrice.global_token_id == global_token_a_id)
                        & (TokenPrice.global_token_source_id == TokenPrice.global_token_source_id)
                        & (TokenPrice.market_type == TokenPrice.market_type)
                    )
                )
            )

            await session.execute(
                update(TokenPrice)
                .where(TokenPrice.global_token_id == global_token_b_id)
                .values(global_token_id=global_token_a_id)
            )

            await session.execute(
                delete(GlobalTokenSource)
                .where(GlobalTokenSource.global_token_id == global_token_b_id)
                .where(
                    GlobalTokenSource.source_id.in_(
                        select(GlobalTokenSource.source_id)
                        .where(GlobalTokenSource.global_token_id == global_token_a_id)
                    )
                )
            )

            await session.execute(
                update(GlobalTokenSource)
                .where(GlobalTokenSource.global_token_id == global_token_b_id)
                .values(global_token_id=global_token_a_id)
            )

            await session.execute(
                update(GlobalMergeCandidate)
                .where(
                    and_(
                        GlobalMergeCandidate.global_token_a_id == global_token_a_id,
                        GlobalMergeCandidate.global_token_b_id == global_token_b_id,
                    )
                )
                .values(status=MergeStatusType.APPROVED.value)
            )

            await session.commit()

        async with self.sessionmaker() as session:
            await session.execute(
                update(GlobalToken)
                .where(GlobalToken.id == global_token_b_id)
                .values(is_deleted=True)
            )
            await session.commit()

    async def decline_candidate(self, global_token_a_id: UUID, global_token_b_id: UUID):
        async with self.sessionmaker() as session:
            stmt = (
                update(GlobalMergeCandidate)
                .filter_by(
                    global_token_a_id=global_token_a_id,
                    global_token_b_id=global_token_b_id,
                )
                .values(status=MergeStatusType.REJECTED.value)
            )
            await session.execute(stmt)
            await session.commit()


class GlobalTokenPairBlacklistRepo(BaseRepo):
    def __init__(self, session: async_sessionmaker):
        from infrastructure.database.models.tokens import GlobalTokenPairBlacklist
        super().__init__(session, GlobalTokenPairBlacklist)  # noqa

    async def is_pair_blacklisted(self, gt_a_id: UUID, gt_b_id: UUID) -> bool:
        """Check if a pair is blacklisted (order-independent)"""
        async with self.sessionmaker() as session:
            from infrastructure.database.models.tokens import GlobalTokenPairBlacklist
            stmt = select(GlobalTokenPairBlacklist.id).where(
                or_(
                    and_(
                        GlobalTokenPairBlacklist.global_token_a_id == gt_a_id,
                        GlobalTokenPairBlacklist.global_token_b_id == gt_b_id
                    ),
                    and_(
                        GlobalTokenPairBlacklist.global_token_a_id == gt_b_id,
                        GlobalTokenPairBlacklist.global_token_b_id == gt_a_id
                    )
                )
            ).limit(1)
            result = await session.execute(stmt)
            return result.scalar_one_or_none() is not None

    async def add_pair(self, gt_a_id: UUID, gt_b_id: UUID, reason: str | None = None, created_by: str | None = None) -> None:
        """Add a blacklist entry (always stores with a < b for consistency)"""
        async with self.sessionmaker() as session:
            from infrastructure.database.models.tokens import GlobalTokenPairBlacklist
            # Ensure consistent ordering
            a_id, b_id = (gt_a_id, gt_b_id) if gt_a_id < gt_b_id else (gt_b_id, gt_a_id)
            
            stmt = insert(GlobalTokenPairBlacklist).values(
                global_token_a_id=a_id,
                global_token_b_id=b_id,
                reason=reason,
                created_by=created_by
            ).on_conflict_do_nothing(
                constraint=uq_pair_blacklist
            )
            await session.execute(stmt)
            await session.commit()
