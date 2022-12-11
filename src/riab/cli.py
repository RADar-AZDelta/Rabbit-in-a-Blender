# Copyright 2022 RADar-AZDelta
# SPDX-License-Identifier: gpl3+

# pylint: disable=unsubscriptable-object
import logging
import logging.config
import os
import sys
import traceback
from argparse import ArgumentParser
from tempfile import NamedTemporaryFile, _TemporaryFileWrapper
from typing import Optional

from .etl import (
    Achilles,
    Cleanup,
    CreateEtlFolders,
    CreateOmopDb,
    DataQuality,
    DataQualityDashboard,
    Etl,
    ImportVocabularies,
)
from .etl.bigquery import (
    BigQueryAchilles,
    BigQueryCleanup,
    BigQueryCreateEtlFolders,
    BigQueryCreateOmopDb,
    BigQueryDataQuality,
    BigQueryDataQualityDashboard,
    BigQueryEtl,
    BigQueryImportVocabularies,
)


def cli() -> None:
    """Main entry point of application"""

    with init_logging() as logger_file_handle:
        try:
            parser = _contstruct_argument_parser()
            args = parser.parse_args()

            logging.warning("Logs are written to %s", logger_file_handle.name)

            if __debug__:
                print(args)

            if args.verbose:
                logging.getLogger().setLevel(logging.DEBUG)

            bigquery_kwargs = None
            match args.db_engine:
                case "BigQuery":
                    bigquery_kwargs = {
                        "credentials_file": args.google_credentials_file,
                        "project_id": args.google_project_id,
                        "location": args.google_location,
                        "dataset_id_raw": args.bigquery_dataset_id_raw,
                        "dataset_id_work": args.bigquery_dataset_id_work,
                        "dataset_id_omop": args.bigquery_dataset_id_omop,
                        "bucket_uri": args.google_cloud_storage_bucket_uri,
                    }
                case _:
                    raise ValueError("Not a supported database engine")

            if args.create_db:  # create OMOP CDM Database
                create_omop_db: Optional[CreateOmopDb] = None
                match args.db_engine:
                    case "BigQuery":
                        create_omop_db = BigQueryCreateOmopDb(
                            cdm_folder_path=args.run_etl or args.create_folders,
                            **bigquery_kwargs,
                        )
                        create_omop_db.run()
                    case _:
                        raise ValueError("Not a supported database engine")
            elif args.create_folders:  # create the ETL folder structure
                create_folders: Optional[CreateEtlFolders] = None
                match args.db_engine:
                    case "BigQuery":
                        create_folders = BigQueryCreateEtlFolders(
                            cdm_folder_path=args.run_etl or args.create_folders,
                            **bigquery_kwargs,
                        )
                        create_folders.run()
                    case _:
                        raise ValueError("Not a supported database engine")
            elif args.import_vocabularies:  # impoprt OMOP CDM vocabularies
                import_vocabularies: Optional[ImportVocabularies] = None
                match args.db_engine:
                    case "BigQuery":
                        import_vocabularies = BigQueryImportVocabularies(
                            cdm_folder_path=args.run_etl or args.create_folders,
                            **bigquery_kwargs,
                        )
                        import_vocabularies.run(args.import_vocabularies)
                    case _:
                        raise ValueError("Not a supported database engine")
            elif args.run_etl:  # run ETL
                etl: Optional[Etl] = None
                match args.db_engine:
                    case "BigQuery":
                        etl = BigQueryEtl(
                            cdm_folder_path=args.run_etl or args.create_folders,
                            only_omop_table=args.table,
                            skip_usagi_and_custom_concept_upload=args.skip_usagi_and_custom_concept_upload,
                            **bigquery_kwargs,
                        )
                        etl.run()
                    case _:
                        raise ValueError("Not a supported database engine")
            elif args.cleanup:  # cleanup OMOP DB
                cleanup: Optional[Cleanup] = None
                match args.db_engine:
                    case "BigQuery":
                        cleanup = BigQueryCleanup(
                            cdm_folder_path=args.run_etl or args.create_folders,
                            clear_auto_generated_custom_concept_ids=args.clear_auto_generated_custom_concept_ids,
                            **bigquery_kwargs,
                        )
                        cleanup.run(args.cleanup)
                    case _:
                        raise ValueError("Not a supported database engine")
            elif args.data_quality:  # check data quality
                data_quality: Optional[DataQuality] = None
                match args.db_engine:
                    case "BigQuery":
                        data_quality = BigQueryDataQuality(
                            cdm_folder_path=args.run_etl or args.create_folders,
                            json_path=args.json,
                            **bigquery_kwargs,
                        )
                        data_quality.run()
                    case _:
                        raise ValueError("Not a supported database engine")
            elif args.data_quality_dashboard:  # view data quality results
                data_quality_dashboard: Optional[DataQualityDashboard] = None
                match args.db_engine:
                    case "BigQuery":
                        data_quality_dashboard = BigQueryDataQualityDashboard(
                            port=args.port if args.port else 8050,
                            **bigquery_kwargs,
                        )
                        data_quality_dashboard.run()
                    case _:
                        raise ValueError("Not a supported database engine")
            elif args.achilles:  # run descriptive statistics
                achilles: Optional[Achilles] = None
                match args.db_engine:
                    case "BigQuery":
                        achilles = BigQueryAchilles(
                            scratch_database_schema=args.bigquery_dataset_id_work,
                            results_database_schema=args.bigquery_dataset_id_omop,
                            cdm_database_schema=args.bigquery_dataset_id_omop,
                            temp_emulation_schema=args.bigquery_dataset_id_work,
                            **bigquery_kwargs,
                        )
                        achilles.run()
                    case _:
                        raise ValueError("Not a supported database engine")
            else:
                raise Exception("Unknown ETL command!")

        except Exception:
            logging.error(traceback.format_exc())
            if __debug__:
                breakpoint()
        finally:
            logging.warning("Logs were written to %s", logger_file_handle.name)


def _contstruct_argument_parser() -> ArgumentParser:
    """Constructs the argument parser"""

    # parser for the required named arguments
    required_parser = MyParser(add_help=False)
    required_parser.add_argument(
        "-v",
        "--verbose",
        help="Verbose logging (logs are also writen to a log file in the systems tmp folder)",
        action="store_true",
    )

    required_named = required_parser.add_argument_group("required named arguments")
    required_named.add_argument(
        "-d",
        "--db-engine",
        nargs="?",
        default="BigQuery",
        choices=["BigQuery"],
        type=str,
        help="""The database engine technology the ETL is running on.
        Each database engine has its own legacy SQL dialect, so the generated ETL queries can be different for
        each database engine. For the moment only BigQuery is supported, yet 'Rabbit in a Blender' has an open design,
        so in the future other database engines can be added easily.""",
        metavar="DB-ENGINE",
        required=not bool(set(sys.argv) & {"-h", "--help"}),
    )
    args, _ = required_parser.parse_known_args()

    # parser for the optional arguments
    command_parser = MyParser(
        add_help=False,
        parents=[required_parser],
    )
    commands_group = command_parser.add_argument_group("ETL commands")
    commands_group.add_argument(
        "-cd", "--create-db", help="Create the OMOP CDM tables", action="store_true"
    )
    commands_group.add_argument(
        "-cf",
        "--create-folders",
        help="Create the ETL folder structure that will hold your queries, Usagi CSV's an custom concept CSV's.",
        nargs="?",
        type=str,
        metavar="PATH",
    )
    commands_group.add_argument(
        "-i",
        "--import-vocabularies",
        nargs="?",
        type=str,
        help="""Extracts the vocabulary zip file (downloaded from the Athena website) and imports it
        into the OMOP CDM database.""",
        metavar="VOCABULARIES_ZIP_FILE",
    )
    commands_group.add_argument(
        "-r",
        "--run-etl",
        help="Runs the ETL, pass the path to ETL folder structure that holds your queries, Usagi CSV's an custom concept CSV's.",  # noqa: E501 # pylint: disable=line-too-long
        nargs="?",
        type=str,
        metavar="PATH",
    )
    commands_group.add_argument(
        "-c",
        "--cleanup",
        nargs="?",
        const="all",
        choices=[
            "all",
            "metadata",
            "cdm_source",
            "vocabulary",
            "location",
            "care_site",
            "provider",
            "person",
            "observation_period",
            "visit_occurrence",
            "visit_detail",
            "condition_occurrence",
            "drug_exposure",
            "procedure_occurrence",
            "device_exposure",
            "measurement",
            "observation",
            "death",
            "note",
            "note_nlp",
            "specimen",
            "fact_relationship",
            "payer_plan_period",
            "cost",
            "episode",
            "episode_event",
        ],
        type=str,
        help="""Cleanup all the OMOP tables (ALL or no TABLE parameter), or just one.
        Be aware that the cleanup of a single table can screw up foreign keys!
        For instance cleaning up only the 'Person' table,
        will result in clicical results being mapped to the wrong persons!!!!""",
        metavar="TABLE",
    )
    commands_group.add_argument(
        "-dq",
        "--data-quality",
        help="Check the data quality.",
        action="store_true",
    )
    commands_group.add_argument(
        "-dqd",
        "--data-quality-dashboard",
        help="View the data quality results.",
        action="store_true",
    )
    commands_group.add_argument(
        "-ach",
        "--achilles",
        help="Generate the descriptive statistics.",
        action="store_true",
    )

    command_args, _ = command_parser.parse_known_args()

    parser = MyParser(
        prog="riab",
        description="Rabbit in a Blender: an OMOP CDM ETL tool",
        parents=[command_parser],
    )
    command_options_group = parser.add_argument_group(
        "Run ETL specific command options (-r [PATH], --run-etl [PATH])"
    )

    command_options_group.add_argument(
        "-s",
        "--skip-usagi-and-custom-concept-upload",
        help="""Skips the parsing and uploading of the Usagi and custom concept CSV's.
        This can be usefull if you only want to upload aditional data to the OMOP database, and there were no changes
        to the Usagi CSV's an custom concept CSV's.
        Skipping results in a significant speed boost.""",
        action="store_true",
    )
    command_options_group.add_argument(
        "-t",
        "--table",
        nargs="?",
        choices=[
            "metadata",
            "cdm_source",
            "vocabulary",
            "location",
            "care_site",
            "provider",
            "person",
            "observation_period",
            "visit_occurrence",
            "visit_detail",
            "condition_occurrence",
            "drug_exposure",
            "procedure_occurrence",
            "device_exposure",
            "measurement",
            "observation",
            "death",
            "note",
            "note_nlp",
            "specimen",
            "fact_relationship",
            "payer_plan_period",
            "cost",
            "episode",
            "episode_event",
        ],
        type=str,
        help="""Do only ETL on this specific OMOP CDM table""",
        metavar="TABLE",
    )

    cleanup_group = parser.add_argument_group("Cleanup specific arguments")
    cleanup_group.add_argument(
        "--clear-auto-generated-custom-concept-ids",
        help="""Cleanup the auto generated custom concept ID's (above 2 million). Without this argument, the cleanup command will not clear the mapping table (that maps the custom concept with the auto generated id above 2 million), so that you can use those above 2 million concept id's in your cohort builder, without the fear that those id's will change.""",  # noqa: E501 # pylint: disable=line-too-long
        action="store_true",
    )

    data_quality_group = parser.add_argument_group("Data quality specific arguments")
    data_quality_group.add_argument(
        "--json",
        nargs="?",
        type=str,
        help="""Path to store the data quality result as JSON file.""",
        metavar="PATH",
    )

    data_quality_dashboard_group = parser.add_argument_group(
        "Data quality dashboard specific arguments"
    )
    data_quality_dashboard_group.add_argument(
        "--port",
        nargs="?",
        type=int,
        help="""Port where the data quality dashboard schould listen on.""",
        metavar="PORT",
    )

    bigquery_group = parser.add_argument_group("BigQuery specific arguments")
    bigquery_group.add_argument(
        "--google-credentials-file",
        nargs="?",
        type=str,
        help="""Loads Google credentials from a file""",
        metavar="GOOGLE_CREDENTIALS_FILE",
    )
    bigquery_group.add_argument(
        "--google-project-id",
        nargs="?",
        type=str,
        help="""The Google GCP project id""",
        metavar="GOOGLE_PROJECT_ID",
    )
    bigquery_group.add_argument(
        "--google-location",
        nargs="?",
        default="EU",
        type=str,
        help="""The google locations to store the data (see https://cloud.google.com/about/locations)""",
        metavar="GOOGLE_LOCATION",
    )
    bigquery_group.add_argument(
        "--bigquery-dataset-id-raw",
        nargs="?",
        type=str,
        help="""BigQuery dataset that holds the raw EMR data""",
        required=args.db_engine == "BigQuery"
        and not command_args.create_db
        and not command_args.create_folders
        and not command_args.import_vocabularies
        and not command_args.cleanup
        and not command_args.data_quality
        and not command_args.data_quality_dashboard
        and not command_args.achilles,
        metavar="BIGQUERY_DATASET_ID_RAW",
    )
    bigquery_group.add_argument(
        "--bigquery-dataset-id-work",
        nargs="?",
        type=str,
        help="""BigQuery dataset that will hold ETL housekeeping tables (ex: swap tablet, etc...)""",
        required=args.db_engine == "BigQuery"
        and not command_args.create_db
        and not command_args.create_folders
        and not command_args.data_quality
        and not command_args.data_quality_dashboard
        and not command_args.achilles,
        metavar="BIGQUERY_DATASET_ID_WORK",
    )
    bigquery_group.add_argument(
        "--bigquery-dataset-id-omop",
        nargs="?",
        type=str,
        help="""BigQuery dataset that will hold the final OMOP tables""",
        required=args.db_engine == "BigQuery",
        metavar="BIGQUERY_DATASET_ID_OMOP",
    )
    bigquery_group.add_argument(
        "--google-cloud-storage-bucket-uri",
        nargs="?",
        type=str,
        help="""Google Cloud Storage bucket uri, that will hold the uploaded Usagi and custom concept files.
        (the uri has format 'gs://{bucket_name}/{bucket_path}')""",
        required=args.db_engine == "BigQuery"
        and not command_args.create_db
        and not command_args.create_folders
        and not command_args.data_quality_dashboard,
        metavar="GOOGLE_CLOUD_STORAGE_BUCKET_URI",
    )

    return parser


def init_logging() -> _TemporaryFileWrapper:
    """Initialise logging"""
    # get main logger
    main_logger = logging.getLogger()
    main_logger.setLevel(logging.INFO)

    # formatters
    console_formatter = ColoredFormatter(
        "%(asctime)s: %(name)s: #%(lineno)d: %(levelname)s - %(message)s"
    )
    file_formatter = logging.Formatter(
        "%(asctime)s %(levelname)s %(pathname)s#%(lineno)d %(message)s"
    )

    # console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(console_formatter)
    main_logger.addHandler(console_handler)

    # file handler
    tmp_file_handle = NamedTemporaryFile(
        delete=False, prefix="omop_etl_", suffix=".log"
    )
    file_handler = logging.FileHandler(tmp_file_handle.name)
    file_handler.setFormatter(file_formatter)
    main_logger.addHandler(file_handler)

    return tmp_file_handle


class MyParser(ArgumentParser):
    """Custom ArgumentParser class with better error printing"""

    def error(self, message):
        """Prints a help message to stdout, the error message to stderr and
        exits.

        Args:
            message (string): The error message
        """
        self.print_help()
        sys.stderr.write(f"error: {message}{os.linesep}")
        sys.exit(2)


class ColoredFormatter(logging.Formatter):
    """Logging colored formatter, adapted from https://stackoverflow.com/a/56944256/3638629"""

    grey = "\x1b[38;21m"
    blue = "\x1b[38;5;39m"
    yellow = "\x1b[38;5;226m"
    red = "\x1b[38;5;196m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"

    def __init__(self, fmt):
        super().__init__()
        self.fmt = fmt
        self.FORMATS = {
            logging.DEBUG: self.grey + self.fmt + self.reset,
            logging.INFO: self.blue + self.fmt + self.reset,
            logging.WARNING: self.yellow + self.fmt + self.reset,
            logging.ERROR: self.red + self.fmt + self.reset,
            logging.CRITICAL: self.bold_red + self.fmt + self.reset,
        }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)
