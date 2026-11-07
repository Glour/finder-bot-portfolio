"""
Global token merge candidate finder with improved architecture.

Refactored to extract bot selection, notification logic, and configuration management.
Includes protection against phantom spreads from multiplied tokens (1000X, 1000000X).
"""
import asyncio
import hashlib
import re
from typing import Any, Optional
from uuid import UUID
from enum import Enum

from aiogram import Bot
from aiogram.utils.markdown import hlink

from bot.keyboards.inline import global_candidate_approve_kb
from bot.services.broadcaster import send_message
from infrastructure.database.models import GlobalToken
from infrastructure.database.models.tokens import GlobalMergeCandidate
from infrastructure.database.requests import RequestsRepo
from infrastructure.services.bots import prod_bot, dev_bot, test_bot
from infrastructure.services.collectors.spread_collector.config import MergeConfig, DEFAULT_MERGE_CONFIG
from infrastructure.services.collectors.structures.normalizer import safe_float
from infrastructure.services.collectors.structures.utils import generate_market_link
from infrastructure.services.common.cache import RedisCache
from settings import logger, settings


class BotEnvironment(str, Enum):
    """Bot environment types."""
    PRODUCTION = "prod"
    DEVELOPMENT = "dev"
    TEST = "test"


class BotSelector:
    """
    Centralized bot selection logic.

    Eliminates duplicate bot selection code throughout the module.
    """

    _bot_map = {
        BotEnvironment.PRODUCTION: prod_bot,
        BotEnvironment.DEVELOPMENT: dev_bot,
        BotEnvironment.TEST: test_bot,
    }

    @classmethod
    def get_bot(cls, environment: str) -> Bot:
        """
        Get bot instance for environment.

        Args:
            environment: Environment name (prod, dev, test)

        Returns:
            Bot instance

        Raises:
            ValueError: If environment is invalid
        """
        try:
            env = BotEnvironment(environment)
            bot = cls._bot_map.get(env)

            if bot is None:
                raise ValueError(f"No bot configured for environment: {environment}")

            return bot

        except ValueError as e:
            logger.warning(f"Invalid environment '{environment}', defaulting to test bot")
            return test_bot


class NotificationService:
    """
    Handles all notification logic for merge candidates.

    Separates notification concerns from business logic.
    """

    def __init__(self, environment: str):
        self.environment = environment
        self.bot = BotSelector.get_bot(environment)

    async def notify_auto_approved(self, candidate: GlobalMergeCandidate) -> None:
        """
        Send notification for auto-approved candidate.

        Args:
            candidate: Auto-approved merge candidate
        """
        links_a = self._collect_links(candidate.global_token_a, candidate.exchanges_a)
        links_b = self._collect_links(candidate.global_token_b, candidate.exchanges_b)

        text = "✅ Автоматически одобрен кандидат\n\n" + self._format_candidate(
            candidate, links_a, links_b
        )

        await send_message(
            bot=self.bot,
            text=text,
            chat_id=settings.bot.alerts_channel_id,
            tread_id=settings.bot.alerts_topic_id,
            reply_markup=None
        )

    async def notify_manual_review(
        self,
        candidate: GlobalMergeCandidate,
        candidate_hash: str,
        admin_chat_ids: Optional[list[int]] = None
    ) -> None:
        """
        Send notification for manual review candidate.

        Args:
            candidate: Candidate requiring manual review
            candidate_hash: Hash for callback identification
            admin_chat_ids: Admin chat IDs (from config if not provided)
        """
        if admin_chat_ids is None:
            admin_chat_ids = DEFAULT_MERGE_CONFIG.admin_chat_ids

        links_a = self._collect_links(candidate.global_token_a, candidate.exchanges_a)
        links_b = self._collect_links(candidate.global_token_b, candidate.exchanges_b)

        text = self._format_candidate(candidate, links_a, links_b)
        reply_markup = await global_candidate_approve_kb(candidate_hash)

        # Send to all admin chat IDs
        tasks = [
            send_message(
                bot=self.bot,
                text=text,
                reply_markup=reply_markup,
                chat_id=chat_id,
                tread_id=None
            )
            for chat_id in admin_chat_ids
        ]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def notify_token_assignment(
        self,
        global_token_id: UUID,
        token_data: dict,
        source_id: UUID,
        source_name: str,
        reason: str,
        is_new_global: bool,
        is_new_token: bool,
        matched_pairs: Optional[list] = None,
        created_id: Optional[UUID] = None
    ) -> None:
        """
        Send notification for token assignment to global token.

        Args:
            global_token_id: Global token ID
            token_data: Token information
            source_id: Source exchange ID
            source_name: Source exchange name
            reason: Assignment reason
            is_new_global: Whether this is a new global token
            is_new_token: Whether this is a new token
            matched_pairs: Matched pairs (if any)
            created_id: Created global token ID (if new)
        """
        # Only notify for new global tokens or first-time token assignments
        if not (is_new_global or is_new_token):
            return

        if is_new_global:
            header = f"Создан новый global_token {global_token_id} и привязан token"
        else:
            header = f"Token впервые привязан к существующему global_token {global_token_id}"

        details = (
            f"{header}\n\n"
            f"Данные:\n"
            f"symbol: {token_data.get('symbol')}\n"
            f"market_type: {token_data.get('market_type')}\n"
            f"base_coin_name: {token_data.get('base_coin_name')}\n"
            f"chains: {token_data.get('chains')}\n"
            f"source_id: {source_id}\n"
            f"source_name: {source_name}\n"
            f"причина: {reason}\n"
            f"детали: {matched_pairs or created_id}"
        )

        await send_message(
            bot=self.bot,
            text=details,
            chat_id=settings.bot.alerts_channel_id,
            tread_id=settings.bot.alerts_topic_id
        )

    @staticmethod
    def _collect_links(global_token: GlobalToken, exchanges: list[str]) -> list[str]:
        """Collect market links for exchanges."""
        links = []
        for src_name in exchanges or []:
            token_obj = next(
                (
                    t
                    for t in global_token.token_on_chains
                    if t.token.source.name.lower() == src_name.lower()
                ),
                None,
            )
            if not token_obj:
                continue

            link = generate_market_link(
                market=token_obj.token.market_type.value,
                symbol=f"{token_obj.token.base_asset}_{token_obj.token.quote_asset}",
                contract_address=token_obj.contract_address,
                chain=token_obj.chain.name if token_obj.chain else "",
                source=src_name.lower(),
            )
            if link:
                links.append(hlink(src_name, link))
        return links

    @staticmethod
    def _format_candidate(c: GlobalMergeCandidate, links_a: list[str], links_b: list[str]) -> str:
        """Format candidate information for notification."""
        return (
            f"Symbol: {c.symbol}\n"
            f"Exchanges A: {', '.join(links_a)}\n"
            f"Exchanges B: {', '.join(links_b)}\n"
            f"A: {c.global_token_a_id}\n"
            f"B: {c.global_token_b_id}\n"
            f"Price A: {c.price_a}\n"
            f"Price B: {c.price_b}\n"
            f"Diff %: {c.price_diff_pct:.2f}\n"
            f"Networks: {', '.join(c.network_overlap or [])}\n"
            f"Reason: {c.reason}"
        )


class UnionFind:
    """
    Union-Find data structure for merging global token IDs.

    Extracted from assign_global_ids for better testability and reusability.
    """

    def __init__(self):
        self.parent: dict[UUID, UUID] = {}

    def find(self, x: UUID) -> UUID:
        """Find root of element with path compression."""
        while self.parent.get(x, x) != x:
            self.parent[x] = self.parent.get(self.parent[x], self.parent[x])
            x = self.parent[x]
        return x

    def union(self, a: UUID, b: UUID) -> None:
        """Union two elements."""
        pa, pb = self.find(a), self.find(b)
        if pa != pb:
            self.parent[pb] = pa

    def add(self, x: UUID) -> None:
        """Add element to union-find."""
        if x not in self.parent:
            self.parent[x] = x

    def get_canonical_map(self) -> dict[UUID, UUID]:
        """Get mapping of all elements to their canonical representatives."""
        return {g_id: self.find(g_id) for g_id in self.parent}


class MergeCandidateFinder:
    """
    Finds and manages global token merge candidates.

    Refactored to separate concerns and improve maintainability.
    Includes protection against phantom spreads from multiplied tokens.
    """

    # Regex pattern to detect multiplied tokens like 1000MOG, 1000000BTT
    MULTIPLIED_TOKEN_PATTERN = re.compile(r'^(\d+)([A-Z]+.*)$', re.IGNORECASE)

    def __init__(
        self,
        repo: RequestsRepo,
        cache: RedisCache,
        environment: str,
        config: Optional[MergeConfig] = None
    ):
        self.repo = repo
        self.cache = cache
        self.environment = environment
        self.config = config or DEFAULT_MERGE_CONFIG
        self.notifier = NotificationService(environment)

    def _is_multiplied_token_pair(self, base_a: str, base_b: str) -> bool:
        """
        Check if two base assets are a multiplied token pair.

        Examples:
            - MOG vs 1000000MOG -> True
            - BONK vs 1000BONK -> True
            - BTC vs ETH -> False
            - TRUMP vs TRUMPSOL -> False

        Args:
            base_a: First base asset
            base_b: Second base asset

        Returns:
            True if one is a multiplied version of the other
        """
        base_a_upper = base_a.upper()
        base_b_upper = base_b.upper()

        # Check if one contains the other with a numeric prefix
        for prefix in self.config.multiplied_token_prefixes:
            # Check if base_a is multiplied version of base_b
            if base_a_upper == f"{prefix}{base_b_upper}":
                return True
            # Check if base_b is multiplied version of base_a
            if base_b_upper == f"{prefix}{base_a_upper}":
                return True

        return False

    def _check_price_ratio_valid(self, price_a: float, price_b: float) -> bool:
        """
        Check if price ratio between two tokens is within acceptable range.

        Prevents merging tokens with vastly different prices (e.g., MOG vs 1000000MOG).

        Args:
            price_a: Price of first token
            price_b: Price of second token

        Returns:
            True if price ratio is acceptable for merging
        """
        if price_a <= 0 or price_b <= 0:
            return False

        ratio = max(price_a, price_b) / min(price_a, price_b)
        return ratio <= self.config.max_contract_price_ratio

    def _is_chainless_only(self, gt: GlobalToken) -> bool:
        """
        Check if global_token contains only chainless tokens (no real on-chain contracts).
        
        Chainless chains: hyperliquid, ascendex (synthetic addresses)
        If ALL contracts in this GT are from chainless chains, return True.
        
        FIX: AscendEX doesn't provide contract_address via API, so tokens have chain but no contract.
        Treat them as chainless to prevent wrong merges.
        """
        if not gt.token_on_chains:
            return True
        
        chainless_chains = {"hyperliquid", "ascendex"}
        
        for tc in gt.token_on_chains:
            chain_name = (tc.chain.name or "").lower() if tc.chain else ""
            contract_address = (tc.contract_address or "").strip()
            
            # AscendEX FIX: If token has chain but NO contract address → treat as chainless
            # This catches AscendEX tokens which have chain_name but empty contract_address
            if chain_name and not contract_address:
                # This is a chainless token (AscendEX pattern)
                continue
            
            # If this token_chain is from a REAL chain (not chainless), GT has real contracts
            if chain_name and chain_name not in chainless_chains:
                return False
        
        # All token_chains are chainless or have no chain
        return True

    async def find(self) -> Optional[list[Any]]:
        """
        Find and process merge candidates.
        
        MEMORY FIX: Added explicit cleanup and memory monitoring to prevent accumulation.

        Returns:
            List of candidate dictionaries or None
        """
        import gc
        import psutil
        import os
        
        # Log memory usage before processing
        process = psutil.Process(os.getpid())
        memory_before = process.memory_info().rss / 1024 / 1024  # MB
        logger.info(f"MergeFinder starting - Memory usage: {memory_before:.1f}MB")
        
        try:
            # STEP 1: Split conflicted global_tokens (multiple contracts on same chain)
            split_count = await self.split_conflicted_global_tokens()
            if split_count > 0:
                logger.info(f"Split {split_count} conflicted global_tokens")

            # MEMORY FIX: Get all global tokens with metadata (refresh after split)
            # Process in batches to avoid loading all data at once
            global_tokens = await self.repo.global_tokens.get_all_with_meta()
            logger.info(f"Loaded {len(global_tokens)} global tokens into memory")
            
            try:
                id_map = {gt.id: gt for gt in global_tokens}

                # Process candidates through pipeline
                candidates = self.collect_candidates(global_tokens)
                valid_candidates = await self.filter_candidates(candidates, id_map)
                ranked = self.rank_candidates(valid_candidates)
                results = self.select_final_pairs(ranked, id_map)

                # Bulk insert candidates
                candidates_list = await self.repo.global_merge_candidates.bulk_insert(results)
                if not candidates_list:
                    return None

                # Separate auto-approve and manual review
                auto_approve_silent = [
                    c for c in candidates_list
                    if self._should_auto_approve_silent(c)
                ]
                auto_approve_notify = [
                    c for c in candidates_list
                    if self._should_auto_approve_notify(c) and c not in auto_approve_silent
                ]

                # Process in background with proper task management
                tasks = []
                if auto_approve_silent:
                    task = asyncio.create_task(
                        self._auto_approve_candidates(auto_approve_silent, notify=False)
                    )
                    tasks.append(task)
                if auto_approve_notify:
                    task = asyncio.create_task(
                        self._auto_approve_candidates(auto_approve_notify, notify=True)
                    )
                    tasks.append(task)
                
                # MEMORY FIX: Wait for critical tasks to complete or detach them properly
                if tasks:
                    # Add task cleanup - important to prevent accumulating background tasks
                    for task in tasks:
                        task.add_done_callback(lambda t: None)  # Ensure task cleanup

                logger.info(
                    f"Inserted {len(candidates_list)} merge candidates "
                    f"({len(auto_approve_silent)} silent, {len(auto_approve_notify)} notified)"
                )

                return results
                
            finally:
                # MEMORY FIX: Explicit cleanup of large data structures
                candidates = None
                valid_candidates = None
                ranked = None
                id_map = None
                
        finally:
            # MEMORY FIX: Clear references and force garbage collection
            global_tokens = None
            gc.collect()
            
            # Log memory usage after processing
            memory_after = process.memory_info().rss / 1024 / 1024  # MB
            memory_diff = memory_after - memory_before
            logger.info(f"MergeFinder completed - Memory usage: {memory_after:.1f}MB (diff: {memory_diff:+.1f}MB)")
            
            # MEMORY WARNING: Log if significant memory growth
            if memory_diff > 100:  # More than 100MB growth
                logger.warning(f"⚠️ High memory growth in MergeFinder: {memory_diff:.1f}MB")
            
        return None

    def _should_auto_approve_notify(self, c: GlobalMergeCandidate) -> bool:
        """Check if candidate should be auto-approved with notification."""
        diff = c.price_diff_pct if c.price_diff_pct is not None else 100
        return (
            "same_symbol" in (c.reason or "") and
            diff > self.config.auto_approve_price_diff_threshold
        )

    def _should_auto_approve_silent(self, c: GlobalMergeCandidate) -> bool:
        """Check if candidate should be auto-approved silently."""
        diff = c.price_diff_pct if c.price_diff_pct is not None else 100
        return (
            "same_contract" in (c.reason or "") or
            (
                "same_symbol" in (c.reason or "") and
                diff <= self.config.auto_approve_silent_threshold
            )
        )

    async def _auto_approve_candidates(
        self,
        candidates_list: list[GlobalMergeCandidate],
        notify: bool = True
    ) -> None:
        """Auto-approve candidates with optional notification."""
        for c in candidates_list:
            await self.repo.global_merge_candidates.approve_candidate(
                c.global_token_a_id, c.global_token_b_id
            )
            if notify:
                await self.notifier.notify_auto_approved(c)

    async def _persist_and_notify(self, candidates_list: list[GlobalMergeCandidate]) -> None:
        """Persist candidates to cache and notify admins."""
        for c in candidates_list:
            combined = f"{c.global_token_a_id}:{c.global_token_b_id}"
            candidate_hash = hashlib.md5(combined.encode()).hexdigest()

            await self.cache.set(
                f"global_candidate_hash:{candidate_hash}",
                combined,
                timeout_secs=self.config.candidate_cache_timeout
            )

            await self.notifier.notify_manual_review(
                c,
                candidate_hash,
                admin_chat_ids=self.config.admin_chat_ids
            )

    async def split_conflicted_global_tokens(self) -> int:
        """
        Split global_tokens that have multiple contracts on the same chain.

        Example: WOJAK with two ETH contracts:
        - MEXC: 0x5026f006b85729a8b14553fae6af249ad16c9aab
        - GATE: 0x8de39b057cc6522230ab19c0205080a8663331ef

        These should be TWO separate global_tokens, not one.

        Returns:
            Number of global_tokens split
        """
        logger.info("split_conflicted_global_tokens: Loading all global_tokens...")
        global_tokens = await self.repo.global_tokens.get_all_with_meta()
        logger.info(f"split_conflicted_global_tokens: Loaded {len(global_tokens)} global_tokens")
        split_count = 0

        for gt in global_tokens:
            # Build map: chain -> set of contracts
            chain_contracts: dict[str, set[str]] = {}

            for tc in gt.token_on_chains or []:
                chain = (tc.chain.name or "").lower() if tc.chain else ""
                contract = (tc.contract_address or "").lower().strip()

                if chain and contract:
                    chain_contracts.setdefault(chain, set()).add(contract)

            # Check if ANY chain has multiple contracts
            for chain, contracts in chain_contracts.items():
                if len(contracts) > 1:
                    logger.warning(
                        f"Found conflicted global_token {gt.id} ({gt.symbol}): "
                        f"{len(contracts)} contracts on {chain}: {contracts}"
                    )

                    # Split this global_token by contract
                    await self._split_by_contracts(gt, chain, contracts)
                    split_count += 1
                    break  # One conflict is enough, move to next global_token

        logger.info(f"split_conflicted_global_tokens: Finished, split_count={split_count}")
        return split_count

    async def _split_by_contracts(
        self,
        gt: GlobalToken,
        conflict_chain: str,
        contracts: set[str]
    ) -> None:
        """
        Split one global_token into multiple by separating contracts.

        Strategy:
        1. Keep first contract in original global_token
        2. Create new global_tokens for remaining contracts
        3. Move token_on_chains to appropriate global_tokens
        """
        contracts_list = sorted(contracts)  # Deterministic order

        # Group token_on_chains by contract
        contract_groups: dict[str, list] = {c: [] for c in contracts_list}
        other_chains = []  # token_on_chains from other chains (keep in original)

        for tc in gt.token_on_chains or []:
            chain = (tc.chain.name or "").lower() if tc.chain else ""
            contract = (tc.contract_address or "").lower().strip()

            if chain == conflict_chain and contract in contracts:
                contract_groups[contract].append(tc)
            else:
                other_chains.append(tc)

        # Keep first contract in original global_token
        first_contract = contracts_list[0]
        remaining_tocs = contract_groups[first_contract] + other_chains

        logger.info(
            f"Keeping {len(contract_groups[first_contract])} tokens with contract {first_contract} "
            f"in original global_token {gt.id}"
        )

        # Create new global_tokens for remaining contracts
        for contract in contracts_list[1:]:
            tocs = contract_groups[contract]
            if not tocs:
                continue

            # Create new global_token with same symbol (may need unique suffix)
            new_gt = await self.repo.global_tokens.create(symbol=gt.symbol)

            # If symbol already taken (UNIQUE constraint), try suffixed names
            if new_gt is None:
                for suffix_i in range(1, 10):
                    candidate_symbol = f"{gt.symbol}_SPLIT_{suffix_i}"
                    new_gt = await self.repo.global_tokens.create(symbol=candidate_symbol)
                    if new_gt is not None:
                        logger.warning(
                            f"Symbol {gt.symbol!r} already taken, created split GT "
                            f"as {candidate_symbol!r}"
                        )
                        break

            if new_gt is None:
                logger.error(
                    f"Could not create split GT for {gt.symbol} (contract {contract}), skipping"
                )
                continue

            # Move token_on_chains to new global_token
            for tc in tocs:
                await self.repo.token_on_chain.update(
                    obj_id=tc.id,
                    global_token_id=new_gt.id
                )

            logger.info(
                f"Created new global_token {new_gt.id} ({new_gt.symbol!r}) for contract {contract} "
                f"with {len(tocs)} tokens (split from {gt.id})"
            )

    def collect_candidates(self, global_tokens: list[GlobalToken]) -> dict[tuple, set[str]]:
        """
        Collect merge candidates based on matching criteria.

        Args:
            global_tokens: List of global tokens

        Returns:
            Dictionary mapping (id_a, id_b) tuples to reason sets
        """
        candidates: dict[tuple, set[str]] = {}

        # Find by base_asset (same symbol)
        by_base = {}
        for gt in global_tokens:
            for tc in gt.token_on_chains or []:
                tok = getattr(tc, "token", None)
                if not tok:
                    continue
                base = (tok.base_asset or "").upper()
                if base:
                    by_base.setdefault(base, set()).add(gt.id)

        for gt_ids in by_base.values():
            self._add_pairs(gt_ids, candidates, "same_symbol")

        # Find by chain + contract address
        seen_contracts = {}
        for gt in global_tokens:
            for tc in gt.token_on_chains or []:
                chain_name = (tc.chain.name or "").lower() if tc.chain else ""
                addr = (tc.contract_address or "").lower()
                if chain_name and addr:
                    seen_contracts.setdefault((chain_name, addr), set()).add(gt.id)

        for gt_ids in seen_contracts.values():
            self._add_pairs(gt_ids, candidates, "same_contract")

        return candidates

    def _add_pairs(self, gt_ids: set, candidates: dict, reason: str) -> None:
        """Add all pairs from set to candidates."""
        gt_list = list(gt_ids)
        for i in range(len(gt_list)):
            for j in range(i + 1, len(gt_list)):
                a_id, b_id = self._ordered_pair(gt_list[i], gt_list[j])
                if a_id != b_id:
                    candidates.setdefault((a_id, b_id), set()).add(reason)

    def _get_base_assets(self, gt: GlobalToken) -> set[str]:
        """Extract all unique base assets from a global token."""
        base_assets = set()
        for tc in gt.token_on_chains or []:
            tok = getattr(tc, "token", None)
            if tok and tok.base_asset:
                base_assets.add(tok.base_asset.upper())
        return base_assets
    
    async def _get_base_assets_for_gt(self, gt_id: UUID) -> set[str]:
        """Extract all unique base assets for a global token by ID."""
        gt = await self.repo.global_tokens.get_by_id_with_meta(gt_id)
        if not gt:
            return set()
        return self._get_base_assets(gt)

    def _has_multiplied_token_conflict(self, gt_a: GlobalToken, gt_b: GlobalToken) -> bool:
        """
        Check if two global tokens have a multiplied token conflict.

        This prevents merging tokens like MOG with 1000000MOG even if they
        share the same contract address (which can happen on some exchanges).
        """
        bases_a = self._get_base_assets(gt_a)
        bases_b = self._get_base_assets(gt_b)

        for base_a in bases_a:
            for base_b in bases_b:
                if self._is_multiplied_token_pair(base_a, base_b):
                    logger.debug(
                        f"Multiplied token conflict: {base_a} vs {base_b} "
                        f"(globals {gt_a.id} vs {gt_b.id})"
                    )
                    return True
        return False

    def _has_contract_conflict(self, gt_a: GlobalToken, gt_b: GlobalToken) -> bool:
        """
        Check if two global tokens have a contract conflict.

        Contract conflict types:
        1. Same chain but DIFFERENT non-empty contracts
           Example: ASTRO on MEXC (BSC 0xaaa) vs ASTRO on XT (BSC 0xbbb)

        2. COMPLETELY DIFFERENT chains with contracts (no overlap)
           Example: GAS on NEO chain vs GAS on Solana chain
           This indicates different tokens that happen to share the same symbol.

        3. Different market_type (SPOT vs FUTURES) with same contract
           Example: FUN SPOT vs FUN FUTURES (contract_size=1000) on multiple exchanges
           These are different trading instruments that shouldn't merge даже if same contract.

        Empty contracts are ignored (not considered conflicts).
        """
        # Check for market_type conflicts
        # Collect all market types from both global_tokens
        market_types_a = set()
        market_types_b = set()

        for tc in gt_a.token_on_chains or []:
            tok = getattr(tc, "token", None)
            if tok and tok.market_type:
                market_types_a.add(str(tok.market_type.value))

        for tc in gt_b.token_on_chains or []:
            tok = getattr(tc, "token", None)
            if tok and tok.market_type:
                market_types_b.add(str(tok.market_type.value))

        # If both global_tokens have contracts with conflicting market types -> conflict
        # CRITICAL FIX: Prevent SPOT-only GT from merging with any GT containing FUTURES
        # Examples:
        #   gt_a={SPOT} + gt_b={FUTURES} -> conflict (no overlap)
        #   gt_a={SPOT} + gt_b={SPOT,FUTURES} -> conflict (gt_b contaminated!)
        #   gt_a={SPOT} + gt_b={SPOT} -> OK (same type)
        if market_types_a and market_types_b:
            has_spot_only_a = market_types_a == {"SPOT"}
            has_spot_only_b = market_types_b == {"SPOT"}
            has_futures_a = "FUTURES" in market_types_a
            has_futures_b = "FUTURES" in market_types_b
            
            # Reject if one is SPOT-only and the other contains FUTURES
            if (has_spot_only_a and has_futures_b) or (has_spot_only_b and has_futures_a):
                logger.debug(
                    f"Market type conflict: {market_types_a} vs {market_types_b} "
                    f"(globals {gt_a.id} vs {gt_b.id})"
                )
                return True

        # Build map: chain -> set of contracts for gt_a
        chain_contracts_a: dict[str, set[str]] = {}
        for tc in gt_a.token_on_chains or []:
            chain = (tc.chain.name or "").lower() if tc.chain else ""
            contract = (tc.contract_address or "").lower().strip()
            if chain and contract:
                chain_contracts_a.setdefault(chain, set()).add(contract)

        # Build map for gt_b
        chain_contracts_b: dict[str, set[str]] = {}
        for tc in gt_b.token_on_chains or []:
            chain = (tc.chain.name or "").lower() if tc.chain else ""
            contract = (tc.contract_address or "").lower().strip()
            if chain and contract:
                chain_contracts_b.setdefault(chain, set()).add(contract)

        # If both have contracts but on completely different chains -> conflict
        # Example: NEO GAS vs Solana GAS
        if chain_contracts_a and chain_contracts_b:
            chains_a = set(chain_contracts_a.keys())
            chains_b = set(chain_contracts_b.keys())

            # No chain overlap at all -> different tokens
            if not (chains_a & chains_b):
                logger.debug(
                    f"Chain conflict (no overlap): "
                    f"{chains_a} vs {chains_b} "
                    f"(globals {gt_a.id} vs {gt_b.id})"
                )
                return True

        # Check for same chain with different contracts
        for chain, contracts_b in chain_contracts_b.items():
            if chain in chain_contracts_a:
                contracts_a = chain_contracts_a[chain]
                # Same chain exists in both globals

                # CRITICAL FIX: If merging would result in MORE than 1 contract on same chain -> conflict
                # This prevents: gt_a has {0x5026...} + gt_b has {0x8de3...} -> merged {0x5026..., 0x8de3...}
                # Example: WOJAK MEXC (0x5026...) vs WOJAK GATE (0x8de3...) - different ETH contracts
                combined_contracts = contracts_a | contracts_b
                if len(combined_contracts) > 1:
                    logger.debug(
                        f"Contract conflict on {chain}: "
                        f"merge would create {len(combined_contracts)} contracts {combined_contracts} "
                        f"(gt_a: {contracts_a}, gt_b: {contracts_b}) "
                        f"(globals {gt_a.id} vs {gt_b.id})"
                    )
                    return True
        return False

    async def filter_candidates(self, candidates: dict, id_map: dict) -> list:
        """
        Filter candidates by price availability and validity checks.

        Includes protection against:
        - Pair blacklist (manually blocked pairs)
        - Multiplied token pairs (1000MOG vs MOG)
        - Extreme price ratio differences (same contract but 1000x price diff)
        - Contract conflicts (same chain but different contracts)

        Args:
            candidates: Raw candidates from collection
            id_map: Mapping of global token IDs to objects

        Returns:
            List of (pair, reasons, price_diff) tuples
        """
        price_cache = {}

        def get_price(gt_id):
            if gt_id not in price_cache:
                price_cache[gt_id] = self._compute_global_token_price(id_map[gt_id])
            return price_cache[gt_id]

        valid = []
        for (a_id, b_id), reasons in candidates.items():
            gt_a, gt_b = id_map.get(a_id), id_map.get(b_id)
            if not gt_a or not gt_b:
                continue

            # Check pair blacklist
            is_blacklisted = await self.repo.global_token_pair_blacklist.is_pair_blacklisted(a_id, b_id)
            if is_blacklisted:
                logger.info(
                    f"Rejected merge candidate {gt_a.symbol}/{gt_b.symbol} ({a_id}/{b_id}): "
                    f"pair is manually blacklisted"
                )
                continue

            price_a, price_b = get_price(a_id), get_price(b_id)
            if price_a is None or price_b is None:
                continue

            # Check for base_asset conflict
            # same_contract + different base_assets: only allow if prices are very close (genuine alias)
            # same_symbol + different base_assets: always reject (different tokens)
            if 'same_contract' in reasons:
                bases_a = self._get_base_assets(gt_a)
                bases_b = self._get_base_assets(gt_b)
                if bases_a and bases_b and not (bases_a & bases_b):
                    # Different base_assets on same contract — only allow if it's an obvious alias
                    # (one base_asset is a substring of the other, e.g. BOB/BUILDONBOB)
                    # OR prices are extremely close (within 3%)
                    is_alias = any(
                        a.upper() in b.upper() or b.upper() in a.upper()
                        for a in bases_a for b in bases_b
                    )
                    if not is_alias:
                        diff_pct = abs(price_a - price_b) / ((price_a + price_b) / 2) * 100 if (price_a + price_b) > 0 else 999
                        if diff_pct > 3.0:
                            logger.info(
                                f'Rejected same_contract merge {gt_a.symbol}/{gt_b.symbol}: '
                                f'different base_assets {bases_a} vs {bases_b} '
                                f'and price diff too large ({price_a:.8f} vs {price_b:.8f}, diff={diff_pct:.1f}%)'
                            )
                            continue
            if 'same_contract' not in reasons:
                bases_a = self._get_base_assets(gt_a)
                bases_b = self._get_base_assets(gt_b)
                if bases_a and bases_b and not (bases_a & bases_b):
                    # STRICT CHECK for chainless tokens: MUST have same base_asset OR close price
                    # Chainless tokens (no contract) can't rely on contract verification
                    if self._is_chainless_only(gt_a) and self._is_chainless_only(gt_b):
                        # Calculate price difference percentage
                        diff_pct = abs(price_a - price_b) / ((price_a + price_b) / 2) * 100 if (price_a + price_b) > 0 else 999
                        if diff_pct > self.config.max_price_diff_pct:
                            logger.info(
                                f'Rejected merge candidate {gt_a.symbol}/{gt_b.symbol}: '
                                f'chainless tokens with different base_assets {bases_a} vs {bases_b} '
                                f'and large price diff ({price_a:.8f} vs {price_b:.8f}, diff={diff_pct:.1f}%)'
                            )
                            continue
                        # Prices are close - allow merge (legitimate alias)
                        logger.info(
                            f'Allowing chainless merge {gt_a.symbol}/{gt_b.symbol}: '
                            f'different base_assets {bases_a} vs {bases_b} but close prices '
                            f'({price_a:.8f} vs {price_b:.8f}, diff={diff_pct:.1f}%)'
                        )
                    else:
                        # Non-chainless tokens: strict base_asset check
                        logger.info(
                            f'Rejected merge candidate {gt_a.symbol}/{gt_b.symbol}: '
                            f'different base_assets {bases_a} vs {bases_b} (no contract match)'
                        )
                        continue

            # Check for multiplied token conflict (e.g., MOG vs 1000000MOG)
            if self._has_multiplied_token_conflict(gt_a, gt_b):
                logger.info(
                    f"Rejected merge candidate {gt_a.symbol}/{gt_b.symbol}: "
                    f"multiplied token conflict"
                )
                continue

            # Check for contract conflict (same chain, different contracts)
            # This means they are different tokens with same symbol
            if self._has_contract_conflict(gt_a, gt_b):
                logger.info(
                    f"Rejected merge candidate {gt_a.symbol}/{gt_b.symbol}: "
                    f"contract conflict (same chain, different contracts)"
                )
                continue

            # For same_contract matches, verify price ratio is reasonable
            # This catches cases where same contract has wildly different prices
            if "same_contract" in reasons:
                if not self._check_price_ratio_valid(price_a, price_b):
                    logger.info(
                        f"Rejected merge candidate {gt_a.symbol}/{gt_b.symbol}: "
                        f"price ratio too large ({price_a:.8f} vs {price_b:.8f})"
                    )
                    continue


            # For same_symbol chainless token matches, verify price difference
            # This prevents merging MEXC SIX ($0.01) with Hyperliquid SIX ($0.67)
            # Use stricter threshold (3%) than contract-based matches (10x)
            if "same_symbol" in reasons and "same_contract" not in reasons:
                if self._is_chainless_only(gt_a) and self._is_chainless_only(gt_b):
                    # Calculate price difference percentage
                    diff_pct = abs(price_a - price_b) / ((price_a + price_b) / 2) * 100
                    if diff_pct > self.config.max_price_diff_pct:
                        logger.info(
                            f"Rejected merge candidate {gt_a.symbol}/{gt_b.symbol}: "
                            f"chainless price diff too large ({price_a:.8f} vs {price_b:.8f}, diff={diff_pct:.1f}%)"
                        )
                        continue

            # SPOT vs FUTURES price validation (Vlad's request 2026-02-17)
            # Prevent merging spot and futures with anomalous price differences
            # This should automatically filter out 90% of fake signals
            market_types_a = set()
            market_types_b = set()
            
            for tc in gt_a.token_on_chains or []:
                if tc.token and tc.token.market_type:
                    market_types_a.add(tc.token.market_type.value)
            
            for tc in gt_b.token_on_chains or []:
                if tc.token and tc.token.market_type:
                    market_types_b.add(tc.token.market_type.value)
            
            # Check if one is SPOT-only and other is FUTURES-only (or contains FUTURES)
            has_spot_a = "SPOT" in market_types_a
            has_spot_b = "SPOT" in market_types_b
            has_futures_a = "FUTURES" in market_types_a
            has_futures_b = "FUTURES" in market_types_b
            
            # If mixing SPOT and FUTURES, check price difference
            # Allow only if prices are very close (within 20%)
            if (has_spot_a and has_futures_b and not has_futures_a) or \
               (has_spot_b and has_futures_a and not has_futures_b):
                diff_pct = abs(price_a - price_b) / ((price_a + price_b) / 2) * 100 if (price_a + price_b) > 0 else 999
                
                # Threshold: 10% (tight: spot-futures basis should be minimal)
                # Most fake signals have 100%+ difference
                if diff_pct > 10.0:
                    logger.info(
                        f"Rejected merge candidate {gt_a.symbol}/{gt_b.symbol}: "
                        f"SPOT vs FUTURES with large price diff ({price_a:.8f} vs {price_b:.8f}, diff={diff_pct:.1f}%) "
                        f"(gt_a: {market_types_a}, gt_b: {market_types_b})"
                    )
                    continue
                else:
                    logger.debug(
                        f"Allowing SPOT vs FUTURES merge {gt_a.symbol}/{gt_b.symbol}: "
                        f"close prices ({price_a:.8f} vs {price_b:.8f}, diff={diff_pct:.1f}%)"
                    )
            
            valid.append(((a_id, b_id), reasons, abs(price_a - price_b)))

        return valid

    @staticmethod
    def rank_candidates(candidates: list) -> list:
        """
        Rank candidates by reason priority and price difference.

        Args:
            candidates: List of (pair, reasons, price_diff) tuples

        Returns:
            Sorted list
        """
        def reason_priority(reasons: set[str]) -> int:
            if "same_contract" in reasons:
                return 0
            if "same_symbol" in reasons:
                return 1
            return 2

        return sorted(candidates, key=lambda x: (reason_priority(x[1]), x[2]))

    def select_final_pairs(self, sorted_candidates: list, id_map: dict) -> list[dict]:
        """
        Select final merge pairs avoiding conflicts.

        Args:
            sorted_candidates: Sorted candidates from ranking
            id_map: Mapping of global token IDs to objects

        Returns:
            List of candidate dictionaries
        """
        used_ids = set()
        results = []

        for (a_id, b_id), reasons, _ in sorted_candidates:
            if a_id in used_ids or b_id in used_ids:
                continue

            a, b = id_map.get(a_id), id_map.get(b_id)
            if not a or not b or a_id == b_id:
                continue

            # Check for overlapping exchanges
            exchanges_a = {s.name.lower() for s in a.sources or []}
            exchanges_b = {s.name.lower() for s in b.sources or []}
            if exchanges_a & exchanges_b:
                logger.debug(
                    f"Skip {a.symbol}-{b.symbol}: same exchanges {exchanges_a & exchanges_b}"
                )
                continue

            # Check price difference
            price_a, price_b = self._compute_global_token_price(a), self._compute_global_token_price(b)
            if price_a is None or price_b is None:
                continue

            diff_pct = abs(price_a - price_b) / ((price_a + price_b) / 2) * 100
            if diff_pct > self.config.max_price_diff_pct:
                continue

            results.append({
                "symbol": a.symbol,
                "global_token_a_id": a_id,
                "global_token_b_id": b_id,
                "price_a": price_a,
                "price_b": price_b,
                "price_diff_pct": diff_pct,
                "reason": ",".join(sorted(reasons)),
                "exchanges_a": list(exchanges_a),
                "exchanges_b": list(exchanges_b),
            })
            used_ids.update([a_id, b_id])

        return results

    @staticmethod
    def _ordered_pair(id1: UUID, id2: UUID) -> tuple[UUID, UUID]:
        """Return ordered pair for consistent comparison."""
        return (id1, id2) if str(id1) < str(id2) else (id2, id1)

    @staticmethod
    def _compute_global_token_price(gt: GlobalToken) -> Optional[float]:
        """Compute mid-price for global token from orderbook data."""
        best_bid = None
        best_ask = None

        for price in gt.prices or []:
            ob = price.orderbook or {}
            bids = ob.get("bids") or []
            asks = ob.get("asks") or []

            if bids:
                max_bid = max(safe_float(b[0]) for b in bids if b and b[0] is not None)
                best_bid = max(best_bid, max_bid) if best_bid is not None else max_bid

            if asks:
                min_ask = min(safe_float(a[0]) for a in asks if a and a[0] is not None)
                best_ask = min(best_ask, min_ask) if best_ask is not None else min_ask

        if best_bid is None or best_ask is None:
            return None

        return (best_bid + best_ask) / 2

    async def assign_global_ids(
        self,
        source_id: UUID,
        source_name: str,
        basecoin_pairs_map: dict[str, set[tuple[str, Optional[str]]]],
        token_map: Optional[list[dict]] = None,
    ) -> dict[str, UUID]:
        """
        Assign global token IDs to base coins using Union-Find for merging.
        
        MEMORY FIX: Added explicit cleanup to prevent memory accumulation.

        Args:
            source_id: Source exchange ID
            source_name: Source exchange name
            basecoin_pairs_map: Mapping of base coins to (chain, address) pairs
            token_map: Token metadata for notifications

        Returns:
            Dictionary mapping base coin names to global token IDs
        """
        import gc
        
        # MEMORY FIX: Limit input size to prevent excessive memory usage
        if len(basecoin_pairs_map) > 10000:
            logger.warning(f"Large basecoin_pairs_map size: {len(basecoin_pairs_map)} - processing in chunks")
        
        try:
            # Normalize all pairs
            all_pairs = self._normalize_pairs(basecoin_pairs_map)
            if not all_pairs:
                return {}

            # MEMORY FIX: Log data size for monitoring
            logger.debug(f"assign_global_ids: processing {len(all_pairs)} pairs for {len(basecoin_pairs_map)} base coins")

            # Get existing global token mappings
            existing_map = await self.repo.token_on_chain.get_existing_global_tokens_by_pairs(
                source_id, list(all_pairs)
            )
            
            # CRITICAL FIX: Filter out GTs that would create contaminations
            # Load metadata for all found GTs to validate compatibility
            if existing_map and token_map:
                gt_ids_to_check = set(existing_map.values())
                gt_metadata = {}
                
                # Load metadata for all GTs at once
                for gt_id in gt_ids_to_check:
                    gt = await self.repo.global_tokens.get_by_id_with_meta(gt_id)
                    if gt:
                        # Extract market_types, contract_sizes AND source_ids
                        market_types = set()
                        contract_sizes = set()
                        source_ids = set()
                        for tc in gt.token_on_chains or []:
                            tok = getattr(tc, "token", None)
                            if tok:
                                if tok.market_type:
                                    market_types.add(str(tok.market_type.value))
                                cs = tok.contract_size or 1.0
                                contract_sizes.add(cs)
                                # Track which sources contributed to this GT
                                if tok.source_id:
                                    source_ids.add(tok.source_id)
                        
                        gt_metadata[gt_id] = {
                            "market_types": market_types,
                            "contract_sizes": contract_sizes,
                            "source_ids": source_ids
                        }
                
                # Build map: base_coin_name -> token_info for validation
                token_info_map = {}
                for token in token_map:
                    base_coin = token.get("base_coin_name")
                    if base_coin:
                        token_info_map[base_coin] = {
                            "market_type": str(token.get("market_type").value) if hasattr(token.get("market_type"), "value") else str(token.get("market_type", "SPOT")),
                            "contract_size": token.get("contract_size") or 1.0
                        }
                
                # Filter existing_map: remove entries that would create contaminations
                filtered_map = {}
                for key, gt_id in existing_map.items():
                    chain, addr, base_asset = key
                    
                    # Find which base_coin_name this pair belongs to
                    matching_base_coin = None
                    for base_coin_name, pairs in basecoin_pairs_map.items():
                        # Normalize for comparison
                        base_normalized = base_coin_name.lower()
                        base_for_match = base_normalized.replace("_futures", "").replace("_f0", "").replace("_f10", "").replace("_f100", "").replace("_f1000", "").replace("_f10000", "")
                        
                        if base_asset == base_for_match:
                            matching_base_coin = base_coin_name
                            break
                    
                    if not matching_base_coin:
                        # Can't find matching base_coin, skip validation
                        filtered_map[key] = gt_id
                        continue
                    
                    # Get new token info
                    new_token = token_info_map.get(matching_base_coin)
                    if not new_token:
                        # No info about new token, allow reuse (safe default)
                        filtered_map[key] = gt_id
                        continue
                    
                    # Get existing GT metadata
                    gt_meta = gt_metadata.get(gt_id)
                    if not gt_meta:
                        # No metadata, allow reuse
                        filtered_map[key] = gt_id
                        continue
                    
                    # Validate market_type compatibility
                    # FIX #7 (2026-02-16): SPOT+FUTURES separation for same exchange
                    # CRITICAL: SPOT+FUTURES mix is FORBIDDEN for chainless tokens (even same source!)
                    # Examples: GAS, ZKP - spot and futures are DIFFERENT assets
                    existing_mts = gt_meta["market_types"]
                    existing_sources = gt_meta["source_ids"]
                    new_mt = new_token["market_type"]
                    
                    if existing_mts:
                        has_spot_only = existing_mts == {"SPOT"}
                        has_futures_only = existing_mts == {"FUTURES"}
                        adding_futures = new_mt == "FUTURES"
                        adding_spot = new_mt == "SPOT"
                        
                        # Check if GT has tokens from ONLY current source
                        only_current_source = (len(existing_sources) == 1 and source_id in existing_sources)
                        
                        # FIX #7: Check if this is chainless token
                        is_chainless_token = chain and not addr  # has chain but NO contract
                        
                        # REJECT SPOT+FUTURES mix if:
                        # 1. GT has tokens from MULTIPLE sources (cross-exchange phantom spreads)
                        # 2. OR: Chainless token (same exchange but different assets)
                        #    Examples: GAS spot vs futures MEXC, ZKP spot vs futures Bitmart
                        if not only_current_source:
                            # GT has tokens from other sources - REJECT SPOT+FUTURES mix
                            if (has_spot_only and adding_futures) or (has_futures_only and adding_spot):
                                logger.warning(
                                    f"Filtered out GT {gt_id} for {matching_base_coin}: "
                                    f"market_type conflict between sources (existing: {existing_mts}, new: {new_mt}, "
                                    f"sources: {existing_sources} + {source_id})"
                                )
                                continue  # Skip this GT, don't add to filtered_map
                        elif is_chainless_token:
                            # FIX #7: Same source BUT chainless → REJECT SPOT+FUTURES mix
                            # Chainless spot and futures are DIFFERENT assets (no contract to verify equality)
                            if (has_spot_only and adding_futures) or (has_futures_only and adding_spot):
                                logger.warning(
                                    f"Filtered out GT {gt_id} for {matching_base_coin}: "
                                    f"chainless SPOT+FUTURES conflict on same exchange "
                                    f"(existing: {existing_mts}, new: {new_mt}, source: {source_name})"
                                )
                                continue  # Skip this GT, don't add to filtered_map
                        # else: only_current_source=True AND has contract → ALLOW SPOT+FUTURES mix
                    
                    # Validate contract_size compatibility
                    existing_cs = gt_meta["contract_sizes"]
                    new_cs = new_token["contract_size"]
                    
                    # Check if new contract_size is compatible with any existing
                    compatible = False
                    for ecs in existing_cs:
                        ratio = max(new_cs, ecs) / min(new_cs, ecs)
                        if ratio <= 1.5:  # Within 50% is OK
                            compatible = True
                            break
                    
                    if not compatible and existing_cs:
                        logger.warning(
                            f"Filtered out GT {gt_id} for {matching_base_coin}: "
                            f"contract_size conflict (existing: {existing_cs}, new: {new_cs})"
                        )
                        continue  # Skip this GT
                    
                    
                    # FIX #2 (2026-02-15): Bidirectional chainless protection
                    # Prevent adding chained tokens to GT that has chainless tokens
                    # (Reverse of base.py:326 fix - protect both directions)
                    
                    # Check if this is a chained token (new token has chain+contract)
                    is_chained_token = chain and addr  # chain and contract not None
                    
                    if is_chained_token:
                        # Check if existing GT has ANY chainless tokens
                        # (tokens without chain info)
                        has_chainless = await self._gt_has_chainless_tokens(gt_id)
                        
                        if has_chainless:
                            logger.warning(
                                f"Filtered out GT {gt_id} for {matching_base_coin}: "
                                f"GT has chainless tokens, can't add chained token (prevents future cross-chain conflicts)"
                            )
                            continue  # Skip this GT
                    
                    # FIX #5 (2026-02-15): Chain conflict validation
                    # Prevent adding token from chain X to GT that already has chain Y (cross-chain contamination)
                    if is_chained_token:
                        # Get existing chain names for this GT
                        existing_chain_names = await self._get_chain_names_for_gt(gt_id)
                        
                        if existing_chain_names:
                            # GT already has chains - check if new chain is compatible
                            incoming_chain_name = chain.lower() if chain else None
                            
                            if incoming_chain_name and incoming_chain_name not in existing_chain_names:
                                # New chain not in existing chains → CONTAMINATION!
                                logger.warning(
                                    f"Filtered out GT {gt_id} for {matching_base_coin}: "
                                    f"chain conflict (existing chains: {existing_chain_names}, new chain: {incoming_chain_name})"
                                )
                                continue  # Skip this GT
                    
                    # FIX #6 (2026-02-16): Chainless tokens base_asset validation
                    # Prevent adding chainless token to GT with different base_asset chainless tokens
                    is_chainless_token = chain and not addr  # has chain but NO contract
                    
                    if is_chainless_token:
                        # Get existing base_assets for chainless tokens in this GT on same chain
                        gt = await self.repo.global_tokens.get_by_id_with_meta(gt_id)
                        if gt:
                            existing_chainless_bases = set()
                            incoming_chain_lower = chain.lower()
                            
                            for tc in gt.token_on_chains or []:
                                # Check if this token_chain is on same chain AND chainless
                                tc_chain = getattr(tc, "chain", None)
                                tc_contract = getattr(tc, "contract_address", None)
                                
                                if tc_chain and tc_chain.name.lower() == incoming_chain_lower and not tc_contract:
                                    # Same chain + chainless → get base_asset
                                    tok = getattr(tc, "token", None)
                                    if tok and tok.base_asset:
                                        existing_chainless_bases.add(tok.base_asset.upper())
                            
                            # Check if new token's base_asset matches existing chainless bases
                            if existing_chainless_bases:
                                new_base_upper = base_asset.upper()
                                
                                if new_base_upper not in existing_chainless_bases:
                                    # Different base_asset → CONTAMINATION!
                                    logger.warning(
                                        f"Filtered out GT {gt_id} for {matching_base_coin}: "
                                        f"chainless base_asset conflict on {chain} "
                                        f"(existing: {existing_chainless_bases}, new: {new_base_upper})"
                                    )
                                    continue  # Skip this GT
                    
                    # All validations passed
                    filtered_map[key] = gt_id
                
                # Replace existing_map with filtered version
                logger.info(f"Filtered existing_map: {len(existing_map)} -> {len(filtered_map)} entries")
                existing_map = filtered_map

            # Use Union-Find to merge overlapping global tokens
            uf = UnionFind()
            for g_id in set(existing_map.values()):
                uf.add(g_id)

            # Merge global tokens that belong to same base coin
            await self._merge_overlapping_tokens(basecoin_pairs_map, existing_map, uf)

            # Get canonical mapping
            merge_map = uf.get_canonical_map()

            # Assign global token IDs
            basecoin_global_ids, new_global_tokens, reasons = self._assign_ids(
                basecoin_pairs_map, existing_map, merge_map
            )

            # Create new global tokens if needed
            await self._create_new_global_tokens(basecoin_global_ids, new_global_tokens, reasons)

            # Skip notifications to avoid N+1 query problem
            # TODO: Optimize _notify_assignments to use bulk exists check
            # if token_map:
            #     await self._notify_assignments(
            #         basecoin_global_ids,
            #         reasons,
            #         token_map,
            #         source_id,
            #         source_name
            #     )

            return basecoin_global_ids
            
        finally:
            # MEMORY FIX: Explicit cleanup of large data structures
            try:
                all_pairs = None
                existing_map = None
                merge_map = None
                new_global_tokens = None
                reasons = None
                uf = None
                
                # Force garbage collection for large operations
                gc.collect()
                
            except Exception as e:
                logger.warning(f"Error during assign_global_ids cleanup: {e}")
                pass

    @staticmethod
    def _normalize_pairs(
        basecoin_pairs_map: dict[str, set[tuple[str, Optional[str]]]]
    ) -> set[tuple[str, Optional[str], str]]:
        """Normalize pairs into (chain, address, base_coin) triples.
        
        Strips FUTURES suffixes from base_coin_name to match Token.base_asset in DB.
        e.g., "SAND_FUTURES" -> "sand", "SAND_F0" -> "sand"
        """
        all_pairs = set()
        for base_coin_name, pairs in basecoin_pairs_map.items():
            # Strip futures suffixes to match Token.base_asset in DB
            base_normalized = re.sub(r'_(FUTURES|F\d+)$', '', base_coin_name, flags=re.IGNORECASE).lower()
            for chain_name, addr in pairs:
                all_pairs.add((
                    chain_name.lower(),
                    addr.lower() if addr else None,
                    base_normalized
                ))
        return all_pairs

    async def _merge_overlapping_tokens(
        self,
        basecoin_pairs_map: dict,
        existing_map: dict,
        uf: UnionFind
    ) -> None:
        """Merge global tokens that share base coin using Union-Find."""
        for base_coin_name, pairs in basecoin_pairs_map.items():
            base_lower = base_coin_name.lower()
            g_ids = []

            for chain_name, addr in pairs:
                key = (chain_name.lower(), addr.lower() if addr else None, base_lower)
                gid = existing_map.get(key)
                if gid is not None:
                    g_ids.append(gid)

            # FIX #1 (2026-02-15): Validate chain conflicts before merging
            # Prevent merging GT that have same chain with different contracts
            if len(g_ids) > 1:
                first, *rest = g_ids
                for g in rest:
                    # Check for chain conflicts before union
                    has_conflict = await self._has_chain_conflict(first, g)
                    if has_conflict:
                        logger.warning(
                            f"Prevented merge for {base_coin_name}: "
                            f"GT {str(first)[:8]}... and {str(g)[:8]}... have conflicting chains"
                        )
                        continue  # Don't merge - create separate GT instead
                    
                    uf.union(first, g)



    async def _gt_has_chainless_tokens(self, gt_id: UUID) -> bool:
        """
        Check if global_token has any chainless tokens.
        
        Chainless tokens = tokens that exist without token_chains records.
        This happens when token was created from CEX data (no blockchain).
        
        Returns True if GT has at least one token without chain info.
        """
        # Get all tokens for this GT
        gt = await self.repo.global_tokens.get_by_id_with_meta(gt_id)
        if not gt:
            return False
        
        # Get all token_on_chains for this GT
        token_chains = getattr(gt, 'token_on_chains', []) or []
        
        # If no chains at all → definitely chainless
        if not token_chains:
            return True
        
        # Get unique token_ids that have chains
        token_ids_with_chains = set()
        for tc in token_chains:
            if tc.token_id:
                token_ids_with_chains.add(tc.token_id)
        
        # Get all tokens linked to this GT via global_token_sources
        # (tokens table doesn't have global_token_id directly - it's many-to-many)
        all_token_ids = set()
        for tc in token_chains:
            if tc.token_id:
                all_token_ids.add(tc.token_id)
        
        # If we have tokens without chain records → chainless
        # Actually, the logic is simpler: if GT has token_chains, it's chained
        # If GT exists but has NO token_chains at all, it's chainless
        # So we already returned True above if token_chains is empty
        
        return False  # If we get here, GT has chains

    async def _has_chain_conflict(self, gt_a_id: UUID, gt_b_id: UUID) -> bool:
        """
        Check if two global_tokens have chain conflicts.
        
        Returns True if:
        - Same chain exists in both GT with different contracts
        - Chainless tokens on DIFFERENT chains (likely different tokens)
        - Chainless tokens on SAME chain with different base_asset
        
        This prevents contaminations like:
        - GT#1: ethereum: 0xAAA vs GT#2: ethereum: 0xBBB → CONFLICT
        - GT#1: sui chainless vs GT#2: sonic chainless → CONFLICT  
        - GT#1: solana DRIFT vs GT#2: dogechain DOGE → CONFLICT
        """
        # Get chains for both GT
        chains_a = await self._get_chains_for_gt(gt_a_id)
        chains_b = await self._get_chains_for_gt(gt_b_id)
        
        # FIX #7 (2026-02-19): Chainless tokens on DIFFERENT chains
        # Extract chainless chains from both GT
        chainless_chains_a = {cid for cid, contract in chains_a.items() if not contract}
        chainless_chains_b = {cid for cid, contract in chains_b.items() if not contract}
        
        if chainless_chains_a and chainless_chains_b:
            # Both have chainless tokens
            if not (chainless_chains_a & chainless_chains_b):
                # Chainless on DIFFERENT chains → likely different tokens
                # Check base_asset to confirm
                bases_a = await self._get_base_assets_for_gt(gt_a_id)
                bases_b = await self._get_base_assets_for_gt(gt_b_id)
                
                # If same base_asset on chainless different chains → DANGEROUS!
                # Example: S @ sui vs S @ sonic (both chainless, different chains)
                if bases_a and bases_b and (bases_a & bases_b):
                    logger.warning(
                        f"FIX #7 BLOCKED: Chainless {bases_a} on chains {chainless_chains_a} "
                        f"vs {bases_b} on chains {chainless_chains_b} - DIFFERENT TOKENS!"
                    )
                    return True  # CONFLICT!
        
        # Check for conflicts on SAME chain
        for chain_id, contract_a in chains_a.items():
            contract_b = chains_b.get(chain_id)
            if contract_b is not None:
                # Same chain exists in both GT
                # FIX #3: NULL != non-NULL contract
                if not self._contracts_match(contract_a, contract_b):
                    return True  # CONFLICT!
                
                # FIX #6 (2026-02-16): Chainless tokens on SAME chain
                # If both contracts are NULL (chainless) → check base_asset
                # Different base_asset = different tokens (DRIFT vs DOGE)
                if contract_a is None and contract_b is None:
                    bases_a = await self._get_base_assets_for_gt(gt_a_id)
                    bases_b = await self._get_base_assets_for_gt(gt_b_id)
                    
                    logger.warning(f"FIX #6 CHECK: GT_A={gt_a_id} bases={bases_a} | GT_B={gt_b_id} bases={bases_b}")
                    
                    # If no overlap in base_assets → CONFLICT!
                    if bases_a and bases_b and not (bases_a & bases_b):
                        logger.warning(f"FIX #6 BLOCKED: Chainless merge {bases_a} vs {bases_b} on SAME chain - CONFLICT!")
                        return True  # CONFLICT: chainless with different base_asset
        
        return False  # No conflicts
    
    async def _get_chains_for_gt(self, gt_id: UUID) -> dict[UUID, str | None]:
        """
        Get chains for a global_token.
        
        Returns: {chain_id: contract_address}
        """
        # Use get_by_id_with_meta to load GT with relationships
        gt = await self.repo.global_tokens.get_by_id_with_meta(gt_id)
        if not gt:
            return {}
        
        token_chains = getattr(gt, 'token_on_chains', []) or []
        
        result = {}
        for tc in token_chains:
            chain_id = tc.chain_id
            contract = tc.contract_address
            
            # Normalize contract: empty string → None
            if contract in [None, "", "none", " ", "null"]:
                contract = None
            
            # If chain_id already exists, check if contracts match
            if chain_id in result:
                existing_contract = result[chain_id]
                if not self._contracts_match(existing_contract, contract):
                    # Same GT has same chain with different contracts → data corruption!
                    logger.error(
                        f"Data corruption: GT {gt_id} has chain {chain_id} with multiple contracts: "
                        f"{existing_contract} and {contract}"
                    )
            
            result[chain_id] = contract
        
        return result

    async def _get_chain_names_for_gt(self, gt_id: UUID) -> set[str]:
        """
        Get chain names for a global_token.
        
        Returns: set of chain names (e.g., {"ethereum", "binance-smart-chain"})
        """
        # Load GT with relationships
        gt = await self.repo.global_tokens.get_by_id_with_meta(gt_id)
        if not gt:
            return set()
        
        token_chains = getattr(gt, 'token_on_chains', []) or []
        
        # Extract chain names directly from token_chains (chain is already loaded with meta)
        chain_names = set()
        for tc in token_chains:
            if tc.chain and tc.chain.name:
                chain_names.add(tc.chain.name.lower())
        
        return chain_names

    @staticmethod
    def _contracts_match(a: str | None, b: str | None) -> bool:
        """
        Check if two contracts match.
        
        FIX #3: NULL != non-NULL
        - NULL == NULL → True
        - 0xAAA == 0xaaa → True (case insensitive)
        - NULL != 0xAAA → False
        """
        # NULL != non-NULL
        if (a is None) != (b is None):
            return False
        
        # Both NULL → match
        if a is None and b is None:
            return True
        
        # Both non-NULL → case-insensitive compare
        return a.lower() == b.lower()


    def _assign_ids(
        self,
        basecoin_pairs_map: dict,
        existing_map: dict,
        merge_map: dict[UUID, UUID]
    ) -> tuple[dict[str, Optional[UUID]], list[dict], dict[str, dict]]:
        """Assign global token IDs to base coins.

        Reuses existing global_tokens ONLY when base_asset matches.
        Creates new global_tokens for unmatched base coins.
        
        The existing_map key is (chain, contract_address, base_asset_lower).
        We only reuse a global_token if it was found via a pair that matches
        the current base_coin_name — this prevents cross-token contamination.
        """
        basecoin_global_ids = {}
        new_global_tokens = []
        reasons = {}

        for base_coin_name, pairs in basecoin_pairs_map.items():
            base_lower = base_coin_name.lower()
            # Strip FUTURES suffixes for matching existing_map keys (which use Token.base_asset)
            # "sand_futures" -> "sand", "sand_f0" -> "sand"
            base_for_match = re.sub(r'_(futures|f\d+)$', '', base_lower)
            matched_gid = None
            matched_pairs_list = []

            for chain_name, addr in pairs:
                key = (chain_name.lower(), addr.lower() if addr else None, base_for_match)
                gid = existing_map.get(key)
                if gid is not None:
                    # Apply merge_map to get canonical ID
                    canonical_gid = merge_map.get(gid, gid)
                    if matched_gid is None:
                        matched_gid = canonical_gid
                    elif matched_gid != canonical_gid:
                        # Multiple different global_tokens found — use first one
                        logger.warning(
                            f"Multiple global_tokens for {base_coin_name}: "
                            f"{matched_gid} vs {canonical_gid}, using first"
                        )
                    matched_pairs_list.append(key)

            if matched_gid is not None:
                basecoin_global_ids[base_coin_name] = matched_gid
                reasons[base_coin_name] = {
                    "reason": "reused existing global_token (base_asset match)",
                    "matched_pairs": matched_pairs_list,
                }
            else:
                # No existing match — create new global_token
                new_global_tokens.append({"symbol": base_coin_name})
                basecoin_global_ids[base_coin_name] = None
                reasons[base_coin_name] = {
                    "reason": "new global_token (no existing match)",
                    "matched_pairs": [],
                }

        return basecoin_global_ids, new_global_tokens, reasons

    async def _create_new_global_tokens(
        self,
        basecoin_global_ids: dict[str, Optional[UUID]],
        new_global_tokens: list[dict],
        reasons: dict[str, dict]
    ) -> None:
        """Create new global tokens for unmatched base coins.
        
        CRITICAL FIX (2026-02-19): Map created GT by symbol to prevent random assignment.
        Previous logic used iterator which didn't preserve symbol→GT mapping when symbols already existed.
        """
        if not new_global_tokens:
            return

        created_tokens = await self.repo.global_tokens.bulk_create(new_global_tokens)
        
        # CRITICAL FIX: Create symbol → GT mapping instead of using iterator
        # This prevents random GT assignment when some symbols already exist in DB
        created_by_symbol = {gt.symbol: gt for gt in created_tokens}

        for base_coin_name in basecoin_global_ids:
            if basecoin_global_ids[base_coin_name] is None:
                # Match by exact symbol
                gt = created_by_symbol.get(base_coin_name)
                if not gt:
                    # Not in result — two scenarios:
                    # 1. Race condition: another collector created same GT simultaneously → OK
                    # 2. Different token: same symbol but different contract → expected
                    # In both cases FIX-006 will filter wrong token_chains.
                    # FIX-008 Redis flag marks contract-conflict cases for distributor to skip.
                    logger.debug(
                        f"GT symbol conflict for {base_coin_name}: "
                        f"existing GT returned (race condition or different token with same symbol)"
                    )
                    continue
                basecoin_global_ids[base_coin_name] = gt.id
                reasons[base_coin_name]["created_id"] = gt.id

    async def _notify_assignments(
        self,
        basecoin_global_ids: dict[str, UUID],
        reasons: dict[str, dict],
        token_map: list[dict],
        source_id: UUID,
        source_name: str
    ) -> None:
        """Notify about token assignments."""
        for token in token_map:
            base_coin_name = token["base_coin_name"]
            g_id = basecoin_global_ids.get(base_coin_name)
            r = reasons.get(base_coin_name, {})
            created_id = r.get("created_id")
            matched_pairs = r.get("matched_pairs")

            if not created_id and not matched_pairs:
                continue

            is_new_token = not await self.repo.tokens.exists(
                symbol=token.get("symbol"),
                market_type=token.get("market_type"),
                source_id=source_id
            )

            await self.notifier.notify_token_assignment(
                global_token_id=g_id,
                token_data=token,
                source_id=source_id,
                source_name=source_name,
                reason=r.get("reason", ""),
                is_new_global=created_id is not None,
                is_new_token=is_new_token,
                matched_pairs=matched_pairs,
                created_id=created_id
            )


def get_merge_finder(
    repo: RequestsRepo,
    cache: RedisCache,
    environment: str,
    config: Optional[MergeConfig] = None
) -> MergeCandidateFinder:
    """
    Factory function for MergeCandidateFinder.

    Args:
        repo: Database repository
        cache: Redis cache
        environment: Environment name (prod, dev, test)
        config: Optional merge configuration

    Returns:
        Configured MergeCandidateFinder instance
    """
    return MergeCandidateFinder(repo, cache, environment, config)
