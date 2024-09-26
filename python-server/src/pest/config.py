from starlette.config import Config as StarletteConfig
import os

_config = StarletteConfig(os.environ.get("ENV_fILE", ".env"))

class Config():
    @staticmethod
    def is_starlette_debug() -> bool:
        return _config('starlette_debug', cast=bool, default=True)

    @staticmethod
    def port() -> int:
        return _config('starlette_port', cast=int, default=8010)

    @staticmethod
    def host() -> str:
        return _config('starlette_host', cast=str, default="0.0.0.0")

    @staticmethod
    def page_size() -> int:
        return _config('db_pagesize', cast=int, default=1024)

    @staticmethod
    def dbhost() -> str:
        return _config("db_host", cast=str, default="local")

    @staticmethod
    def dbroot() -> str:
        return _config("db_root", cast=str, default="/tmp/pest")


# NOTE possibly make ext configurable
# NOTE this is not the most visually appealing -- to restore original uvicorn color based logging, use
# https://github.com/encode/uvicorn/blob/master/uvicorn/config.py#L66
logging_config = {
    "version": 1,
    "disable_existing_loggers": True,
    "formatters": {
        "default": {
            "format": "{asctime}:{levelname}:{name}:{process}:{message:1.10000}",
            "style": "{",
        },
    },
    "handlers": {
        "default": {
            "formatter": "default",
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stderr",
        },
    },
    "loggers": {
        "uvicorn": {"level": "INFO"},
        "pest": {"level": "INFO"},
        "httpcore": {"level": "ERROR"},
        "httpx": {"level": "ERROR"},
        "": {"level": "WARNING", "handlers": ["default"]},
    },    
} 
