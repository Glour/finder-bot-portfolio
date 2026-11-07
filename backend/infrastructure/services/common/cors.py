from fastapi.middleware.cors import CORSMiddleware

from settings.app_settings import AppSettings


def setup_cors(app, settings: AppSettings):
    if settings.is_development:
        origins = ["*"]
    else:
        origins = [
            "*"
        ]

    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
