# pylint: disable=unsubscriptable-object
import logging
import logging.config
import traceback
from argparse import ArgumentParser
from tempfile import NamedTemporaryFile, _TemporaryFileWrapper

"""Main entry point of application
"""
def main() -> None:
    with init_logging():
        try:
            parser = ArgumentParser(description='RADar AZ Delta: OMOP CDM ETL')
            parser.add_argument('-v',
                                '--verbose',
                                help='Verbose logging',
                                action='store_true')
            args = parser.parse_args()
            print(args)

            if args.verbose:
                logging.getLogger().setLevel(logging.DEBUG)
                
        except Exception:
            logging.error(traceback.format_exc())
            breakpoint()


"""Initialise logging
"""
def init_logging() -> _TemporaryFileWrapper:
    # get main logger
    main_logger = logging.getLogger()
    main_logger.setLevel(logging.INFO)

    # formatters
    default_formatter = logging.Formatter('%(asctime)s: %(name)s: #%(lineno)d: %(levelname)s - %(message)s')
    detailed_formatter = logging.Formatter('%(asctime)s %(levelname)s %(pathname)s#%(lineno)d %(message)s')

    # console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(default_formatter)
    main_logger.addHandler(console_handler)

    # file handler
    tmp_file_handle = NamedTemporaryFile(delete=False, prefix="omop_etl_", suffix=".log")
    print(f"Logs are written to {tmp_file_handle.name}")
    file_handler = logging.FileHandler(tmp_file_handle.name)
    file_handler.setFormatter(detailed_formatter)
    main_logger.addHandler(file_handler)

    return tmp_file_handle


if __name__ == "__main__":
    main()
