import logging
import uvicorn
from pest.config import Config, logging_config

if __name__ == "__main__":
    logging.config.dictConfig(logging_config)
    config = uvicorn.Config(
        "pest.server:app",
        port=Config.port(),
        host=Config.host(),
        log_config=None,
        log_level=None,
        workers=1,
    )
    server = uvicorn.Server(config)
    server.run()
