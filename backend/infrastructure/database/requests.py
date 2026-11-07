from dataclasses import dataclass

from sqlalchemy.ext.asyncio import async_sessionmaker

from infrastructure.database import setup as database_setup
from infrastructure.database.repo.chains import ChainRepo, TokenOnChainRepo
from infrastructure.database.repo.signals import SignalHistoryRepo
from infrastructure.database.repo.sources import SourceRepo, GlobalTokenSourceRepo, ExchangeBlacklistRepo
from infrastructure.database.repo.tokens import TokenRepo, TokenPriceRepo, DexPairRepo, GlobalTokenRepo, \
    TokenBlacklistRepo, GlobalMergeCandidateRepo, GlobalTokenPairBlacklistRepo


@dataclass
class RequestsRepo:
    """
    Repository for handling database operations. This class holds all the repositories for the database models.

    You can add more repositories as properties to this class, so they will be easily accessible.
    """

    sessionmaker: async_sessionmaker

    @property
    def tokens(self) -> TokenRepo:
        return TokenRepo(self.sessionmaker)

    @property
    def global_tokens(self) -> GlobalTokenRepo:
        return GlobalTokenRepo(self.sessionmaker)

    @property
    def chains(self) -> ChainRepo:
        return ChainRepo(self.sessionmaker)

    @property
    def token_on_chain(self) -> TokenOnChainRepo:
        return TokenOnChainRepo(self.sessionmaker)

    @property
    def sources(self) -> SourceRepo:
        return SourceRepo(self.sessionmaker)

    @property
    def global_token_sources(self) -> GlobalTokenSourceRepo:
        return GlobalTokenSourceRepo(self.sessionmaker)

    @property
    def token_prices(self) -> TokenPriceRepo:
        return TokenPriceRepo(self.sessionmaker)

    @property
    def token_blacklist(self) -> TokenBlacklistRepo:
        return TokenBlacklistRepo(self.sessionmaker)

    @property
    def dex_pairs(self) -> DexPairRepo:
        return DexPairRepo(self.sessionmaker)

    @property
    def global_merge_candidates(self) -> GlobalMergeCandidateRepo:
        return GlobalMergeCandidateRepo(self.sessionmaker)

    @property
    def exchange_blacklist(self) -> ExchangeBlacklistRepo:
        return ExchangeBlacklistRepo(self.sessionmaker)

    @property
    def signal_history(self) -> SignalHistoryRepo:
        return SignalHistoryRepo(self.sessionmaker)

    @property
    def global_token_pair_blacklist(self) -> GlobalTokenPairBlacklistRepo:
        return GlobalTokenPairBlacklistRepo(self.sessionmaker)


def get_database_repo() -> RequestsRepo:
    if database_setup.async_session is None:
        raise RuntimeError("async_session not initialized")
    return RequestsRepo(sessionmaker=database_setup.async_session)
