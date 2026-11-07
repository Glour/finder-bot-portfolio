import logging

from settings.app_settings import get_app_settings, AppSettings

settings: AppSettings = get_app_settings()
logger = logging.getLogger(__name__)
