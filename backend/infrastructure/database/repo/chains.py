from collections import defaultdict
from typing import Any
from uuid import UUID

from sqlalchemy import select, func, and_, or_, delete, case
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.sql.functions import coalesce

from infrastructure.database.models import Chain, TokenOnChain, Token, GlobalToken
from infrastructure.database.repo.base import BaseRepo


class ChainRepo(BaseRepo):
    def __init__(self, session: async_sessionmaker):
        super().__init__(session, Chain)  # noqa

    async def get_by_names(self, names: list[str]) -> dict[str, Chain]:
        async with self.sessionmaker() as session:
            stmt = (
                select(Chain)
                .where(Chain.name.in_(names))
            )

            result = await session.execute(stmt)
            return {chain.name: chain for chain in result.scalars().all()}

    async def get_or_create_bulk_by_names(self, names: list[str]) -> dict[str, Chain]:
        async with self.sessionmaker() as session:
            existing = await self.get_by_names(names)
            missing = [name for name in names if name not in existing]

            if missing:
                stmt = (
                    insert(Chain)
                    .values([{"name": name} for name in missing])
                    .returning(Chain)
                )
                result = await session.execute(stmt)
                await session.commit()
                for chain in result.scalars().all():
                    existing[chain.name] = chain

            return existing

    async def get_or_create_bulk_by_names_and_block_time(self, chains: list[dict[str, Any]]) -> dict[str, Chain]:
        async with self.sessionmaker() as session:
            names = [chain["name"] for chain in chains]
            existing = await self.get_by_names(names)
            existing_names = set(existing)

            missing = [chain for chain in chains if chain["name"] not in existing_names]
            if missing:
                stmt = (
                    insert(Chain)
                    .values(missing)
                    .returning(Chain)
                )
                result = await session.execute(stmt)
                await session.commit()
                for chain in result.scalars().all():
                    existing[chain.name] = chain

            return existing


class TokenOnChainRepo(BaseRepo):
    def __init__(self, session: async_sessionmaker):
        super().__init__(session, TokenOnChain)  # noqa

    @staticmethod
    def chunked(iterable, size):
        for i in range(0, len(iterable), size):
            yield iterable[i:i + size]

    async def bulk_upsert_token_on_chains(self, data: list[dict]) -> list[UUID]:
        max_args = 32767
        fields_per_row = 14
        max_rows = max_args // fields_per_row

        if not data:
            return []

        extra_fields = ["chain_name", "base_coin_name", "price_usd", "liquidity_usd",
                         "deposit_enabled_changed_at", "withdraw_enabled_changed_at"]
        # Skip rows where contract_address is None — the DB column is NOT NULL.
        # Such rows arise when a token has a chain but no known contract (e.g. HYPE on HyperEvm).
        # We cannot insert them and they're not useful anyway.
        insert_data = []
        for row in data:
            contract = row.get("contract_address")
            if contract is None:
                continue  # Skip — NOT NULL constraint would reject this row
            insert_data.append(
                {k: v for k, v in row.items() if v is not None and k not in extra_fields}
            )

        all_ids = []

        async with self.sessionmaker() as session:
            for chunk in self.chunked(insert_data, max_rows):
                stmt = (
                    insert(TokenOnChain)
                    .values(chunk)
                    .on_conflict_do_update(
                        index_elements=["token_id", "chain_id"],
                        set_={
                            "contract_address": insert(TokenOnChain).excluded.contract_address,
                            "global_token_id": insert(TokenOnChain).excluded.global_token_id,
                            "deposit_enabled": insert(TokenOnChain).excluded.deposit_enabled,
                            "min_deposit_amount": insert(TokenOnChain).excluded.min_deposit_amount,
                            "deposit_confirm": insert(TokenOnChain).excluded.deposit_confirm,
                            "deposit_tax": insert(TokenOnChain).excluded.deposit_tax,
                            "withdraw_enabled": insert(TokenOnChain).excluded.withdraw_enabled,
                            "min_withdraw_amount": insert(TokenOnChain).excluded.min_withdraw_amount,
                            "withdrawal_fee": insert(TokenOnChain).excluded.withdrawal_fee,
                            "withdraw_tax": insert(TokenOnChain).excluded.withdraw_tax,
                            "updated_at": insert(TokenOnChain).excluded.updated_at,
                            # Track when deposit/withdraw status last changed
                            "deposit_enabled_changed_at": case(
                                (
                                    insert(TokenOnChain).excluded.deposit_enabled.is_distinct_from(
                                        TokenOnChain.deposit_enabled
                                    ),
                                    func.now(),
                                ),
                                else_=TokenOnChain.deposit_enabled_changed_at,
                            ),
                            "withdraw_enabled_changed_at": case(
                                (
                                    insert(TokenOnChain).excluded.withdraw_enabled.is_distinct_from(
                                        TokenOnChain.withdraw_enabled
                                    ),
                                    func.now(),
                                ),
                                else_=TokenOnChain.withdraw_enabled_changed_at,
                            ),
                        }
                    )
                    .returning(
                        TokenOnChain.id,
                        TokenOnChain.token_id,
                        TokenOnChain.chain_id,
                        TokenOnChain.contract_address
                    )
                )

                result = await session.execute(stmt)
                all_ids.extend(result.scalars().all())

            await session.commit()

        return all_ids

    async def delete_stale_token_chains(self, token_on_chain_data: list[dict]) -> int:
        """
        Delete stale token_chain entries when a token's blockchain has changed.

        For each token_id in the provided data, deletes all existing token_chain rows
        whose chain_id is NOT in the set of valid chain_ids for that token.

        Root cause fix: bulk_upsert uses ON CONFLICT (token_id, chain_id) DO UPDATE,
        so if a token migrates from chain A to chain B, the old chain-A row is never
        removed — leading to stale contracts re-triggering wrong GT merges.

        Example: Gate ELON_USDT was on Ethereum (Dogelon Mars contract), then Gate
        relisted it as Echelon on Aptos. The ETH row stayed forever and kept merging
        ELON into the Dogelon Mars global_token.

        Args:
            token_on_chain_data: Current valid token_chain entries (list of dicts
                                  with at least 'token_id' and 'chain_id' keys).

        Returns:
            Number of deleted rows.
        """
        if not token_on_chain_data:
            return 0

        # Group by token_id → set of currently valid chain_ids
        token_valid_chains: dict[UUID, set[UUID]] = {}
        for item in token_on_chain_data:
            token_id = item.get("token_id")
            chain_id = item.get("chain_id")
            if token_id and chain_id:
                token_valid_chains.setdefault(token_id, set()).add(chain_id)

        if not token_valid_chains:
            return 0

        deleted_count = 0
        async with self.sessionmaker() as session:
            for token_id, valid_chain_ids in token_valid_chains.items():
                stmt = (
                    delete(TokenOnChain)
                    .where(
                        and_(
                            TokenOnChain.token_id == token_id,
                            TokenOnChain.chain_id.notin_(valid_chain_ids)
                        )
                    )
                )
                result = await session.execute(stmt)
                deleted_count += result.rowcount

            if deleted_count > 0:
                await session.commit()

        return deleted_count

    async def get_existing_global_tokens_by_pairs(
            self,
            source_id: UUID,
            pairs: list[tuple[str, str | None, str | None]]
    ) -> dict[tuple[Any, Any, Any], Any] | dict[Any, Any]:
        if not pairs:
            return {}

        conditions = []
        for chain_name, contract_address, base_coin_name in pairs:
            if contract_address not in [None, "", "none", " ", "null"]:
                # По контракту + base_asset — предотвращает склеивание разных токенов
                conditions.append(
                    and_(
                        func.lower(Chain.name) == chain_name.lower(),
                        func.lower(TokenOnChain.contract_address) == contract_address.lower(),
                        func.lower(Token.base_asset) == base_coin_name.lower()
                    )
                )
            else:
                # без контракта — ищем по (chain, base_asset) БЕЗ фильтра source_id
                # чтобы разные биржи могли переиспользовать один global_token
                # Ценовая валидация происходит в _assign_global_ids_for_chainless_tokens
                conditions.append(
                    and_(
                        func.lower(Chain.name) == chain_name.lower(),
                        func.lower(Token.base_asset) == base_coin_name.lower(),
                        or_(
                            TokenOnChain.contract_address.is_(None),
                            TokenOnChain.contract_address == ""
                        )
                    )
                )

        stmt = (
            select(
                func.lower(Chain.name),
                func.lower(coalesce(TokenOnChain.contract_address, "")),
                TokenOnChain.global_token_id,
                func.lower(coalesce(Token.base_asset, "")),
                func.lower(coalesce(GlobalToken.symbol, ""))
            )
            .join(TokenOnChain.chain)
            .join(TokenOnChain.token)
            .join(GlobalToken, GlobalToken.id == TokenOnChain.global_token_id)
            .where(or_(*conditions))
            .where(GlobalToken.is_deleted == False)  # noqa: E712
        )

        async with self.sessionmaker() as session:
            result = await session.execute(stmt)

        # нормализуем: адрес -> None если пустая строка, base_asset уже lower
        out = {}
        for row in result.all():
            chain = row[0]
            addr = row[1] or None  # "" -> None
            gid = row[2]
            base_asset = row[3] or ""
            gt_symbol = row[4] or ""
            # CRITICAL: validate GT symbol matches base_asset to prevent wrong GT reuse
            # (e.g. WHITEWHALE GT assigned to AVAX tokens, SERV GT assigned to TUP tokens)
            if gt_symbol and base_asset and gt_symbol != base_asset:
                # Allow known aliases (one is substring of other, e.g. BOB/BUILDONBOB)
                is_alias = gt_symbol in base_asset or base_asset in gt_symbol
                if not is_alias:
                    continue  # Skip: GT symbol mismatch — wrong GT, force re-assignment
            # ключ — тройка: (chain, addr_or_None, base_asset_lower)
            out[(chain, addr, base_asset)] = gid

        return out

    async def get_all_contract_addresses_grouped(self, limit: int = None) -> dict[str, list[str]]:
        async with self.sessionmaker() as session:
            stmt = (
                select(Chain.name, TokenOnChain.contract_address)
                .join(Chain, Chain.id == TokenOnChain.chain_id)
                .where(TokenOnChain.contract_address.isnot(None))
                .order_by(Chain.name.desc())
            )

            if limit:
                stmt = stmt.limit(limit)

            result = await session.execute(stmt)
            rows = result.all()

            addresses_by_chain: dict[str, list[str]] = defaultdict(list)
            for chain_name, address in rows:
                addresses_by_chain[chain_name].append(address)

            return dict(addresses_by_chain)

    async def get_token_on_chain_id_map_by_chain_and_address(self) -> dict[tuple[str, Any], tuple[Any, Any]]:
        async with self.sessionmaker() as session:
            stmt = (
                select(TokenOnChain.id, TokenOnChain.token_id, TokenOnChain.chain_id, TokenOnChain.contract_address)
            )

            rows = await session.execute(stmt)

            return {
                (str(row.chain_id), row.contract_address.lower()): (row.id, row.token_id)
                for row in rows.fetchall()
            }

    async def exists_for_global_token(self, global_token_id: UUID) -> bool:
        """
        Check if any token_chains exist for the given global_token_id.
        
        Used to prevent chainless tokens from merging with chained global_tokens.
        
        Args:
            global_token_id: Global token ID to check
            
        Returns:
            True if any token_chains exist for this global_token
        """
        async with self.sessionmaker() as session:
            stmt = (
                select(func.count(TokenOnChain.id))
                .where(TokenOnChain.global_token_id == global_token_id)
            )
            
            result = await session.execute(stmt)
            count = result.scalar_one()
            
            return count > 0
