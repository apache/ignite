"""
JMX Prometheus Exporter helper for ducktests.

Starts jmx_prometheus_javaagent as a JVM agent to expose JVM/Ignite metrics
on :{port}/metrics in Prometheus text format for external scraping.

Works for both Docker and remote nodes:
 - jmx_exporter.jar is expected at JMX_EXPORTER_JAR_PATH on each node
 - jmx_exporter.yml is created in the node's config directory at startup
"""

import os
from dataclasses import dataclass
from importlib.resources import files

JMX_EXPORTER_JAR_PATH = "/opt/jmx_exporter.jar"

JMX_EXPORTER_YML_NAME = "jmx_exporter.yml"


@dataclass(frozen=True)
class JmxExporterParams:
    """
    JMX Prometheus Exporter settings from globals config.

    Example in globals:
        {
            "jmx_exporter": {
                "enabled": true,
                "port": 8083
            }
        }
    """
    enabled: bool
    port: int


def get_jmx_exporter_params(globals_cfg: dict = None) -> JmxExporterParams:
    """
    Read JMX Exporter parameters from globals configuration.
    """
    if globals_cfg is None:
        globals_cfg = {}

    cfg = globals_cfg.get("jmx_exporter", {})
    if not isinstance(cfg, dict):
        return JmxExporterParams(enabled=True, port=8083)

    return JmxExporterParams(
        enabled=bool(cfg.get("enabled", True)),
        port=int(cfg.get("port", 8083)),
    )


def is_jmx_exporter_enabled(globals_cfg: dict = None) -> bool:
    """
    Check if JMX Exporter is enabled in globals.
    """
    return get_jmx_exporter_params(globals_cfg).enabled


def get_jmx_exporter_yml_content() -> str:
    """
    Return the bundled jmx_exporter.yml content as a string.
    """
    file = (files(__package__)
           .joinpath("jmx_exporter.yml"))
    return file.read_text(encoding="utf-8")


def jmx_agent_jvm_opt(port: int = 8083, config_path: str = None, jar_path: str = None) -> str:
    """
    Build -javaagent JVM option for jmx_prometheus_javaagent.

    :param port: HTTP port for /metrics endpoint
    :param config_path: Absolute path to jmx_exporter.yml ON THE NODE
                        (e.g., /mnt/service/config/jmx_exporter.yml)
    :param jar_path: Absolute path to jmx_exporter.jar ON THE NODE
                     (e.g., /opt/jmx_exporter.jar or /mnt/service/jmx_exporter.jar)
                     If None, uses default JMX_EXPORTER_JAR_PATH
    :return: JVM argument string, e.g.
             -javaagent:/opt/jmx_exporter.jar=8083:/mnt/service/config/jmx_exporter.yml
    """
    if config_path is None:
        config_path = JMX_EXPORTER_YML_NAME

    if jar_path is None:
        jar_path = JMX_EXPORTER_JAR_PATH

    return f"-javaagent:{jar_path}={port}:{config_path}"
