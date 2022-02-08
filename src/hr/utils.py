import logging
import subprocess
import platform
import re

from pyspark.conf import SparkConf
from pyspark.sql import SparkSession


def get_windows_os_network_interfaces():
    output = subprocess.check_output("ipconfig /all").decode('utf-8')

    lines = output.splitlines()

    filter_lines = filter(lambda x: x, lines)

    ip_address = ''
    mac_address = ''
    name = ''

    for line in filter_lines:
        # -------------
        # Interface Name

        is_interface_name = re.match(r"^[a-zA-Z0-9].*:$", line)
        if is_interface_name:

            # Check if there's previews values, if so - yield them
            if name and ip_address and mac_address:
                yield {
                    "ip_address": ip_address,
                    "mac_address": mac_address,
                    "name": name,
                }

            ip_address = ''
            mac_address = ''
            name = line.rstrip(':')

        line = line.strip().lower()

        if ':' not in line:
            continue

        value = line.split(':')[-1]
        value = value.strip()

        # -------------
        # IP Address

        is_ip_address = not ip_address and re.match(r'ipv4 address|autoconfiguration ipv4 address|ip address', line)

        if is_ip_address:
            ip_address = value
            ip_address = ip_address.replace('(preferred)', '')
            ip_address = ip_address.strip()

        # -------------
        # MAC Address

        is_mac_address = not ip_address and re.match(r'physical address', line)

        if is_mac_address:
            mac_address = value
            mac_address = mac_address.replace('-', ':')
            mac_address = mac_address.strip()

    if name and ip_address and mac_address:
        yield {
            "ip_address": ip_address,
            "mac_address": mac_address,
            "name": name,
        }


def get_windows_ip():
    for interface in get_windows_os_network_interfaces():
        if "lan" in interface["name"].lower():
            return interface["ip_address"]


def get_host_ip():
    return get_windows_ip() if platform.system() == "Windows" \
        else subprocess.check_output(["hostname", "-i"]).decode(encoding="utf-8").strip()


def get_spark_context(app_name: str) -> SparkSession:
    """
    Helper to manage the `SparkContext` and keep all of our
    configuration params in one place. See below comments for details:
        |_ https://github.com/bitnami/bitnami-docker-spark/issues/18#issuecomment-700628676
    """

    host_ip = get_host_ip()

    conf = SparkConf()

    conf.setAll(
        [
            ("spark.master", "spark://{}:7077".format(host_ip)),
            ("spark.driver.host", host_ip),
            ("spark.submit.deployMode", "client"),
            ("spark.driver.bindAddress", "0.0.0.0"),
            ("spark.ui.showConsoleProgress", "true"),
            ("spark.eventLog.enabled", "false"),
            ("spark.logConf", "false"),
            ("spark.app.name", app_name),
        ]
    )

    return SparkSession.builder.config(conf=conf).getOrCreate()


def init_logger(level=logging.INFO):
    logger = logging.getLogger()
    logging.captureWarnings(True)

    stream_handler = logging.StreamHandler()
    logger.addHandler(stream_handler)

    format_str = "%(asctime)s - %(levelname)s [%(filename)s:%(lineno)d] %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"

    formatter = logging.Formatter(format_str, date_format)
    stream_handler.setFormatter(formatter)

    logger.setLevel(level)

    if logger.hasHandlers():
        logger.handlers.clear()

    logger.addHandler(stream_handler)

    return logger
