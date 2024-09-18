import logging


def setup_logging():
    # Configurações de logging personalizadas
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


# Adicione funções utilitárias conforme necessário
