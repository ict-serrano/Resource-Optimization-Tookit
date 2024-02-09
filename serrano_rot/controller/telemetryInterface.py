import json
import logging
import requests

logger = logging.getLogger("SERRANO.ROT.TelemetryInterface")


class TelemetryInterface:

    def __init__(self, config):
        self.__cth_service = config["cth_service"]

    def get_infrastructure_resources(self, algorithm):
        resources = {}

        logger.info("Query Central Telemetry Handler for infrastructure sources for algorithm '%s'" % algorithm)

        if algorithm == "StoragePolicy":
            resources = self.__query_storage_locations()
        if algorithm == "SimpleMatch" or algorithm == "SecurityTiers":
            resources = self.__query_clusters()
            resources["cluster_deployments"] = self.__query_serrano_deployments()["cluster_deployments"]
        if algorithm == "OnDemandKernel":
            resources = self.__query_clusters_and_kernels()
            resources["kernel_deployments"] = self.__query_on_demand_kernel_deployments()["kernel_deployments"]

        logger.info("Query Central Telemetry Handler - Data returned")
        logger.debug(json.dumps(resources))

        return resources

    def __query_clusters(self):
        data = {}
        try:
            res = requests.get("%s/api/v1/telemetry/central/infrastructure" % self.__cth_service)
            logger.debug(res.text)
            if res.status_code == 200 or res.status_code == 201:
                data = res.json()
        except Exception as e:
            logger.error("Unable to query Central Telemetry Handler for available clusters and kernels")
            logger.error(str(e))
        return data

    def __query_serrano_deployments(self):
        data = {}
        try:
            res = requests.get("%s/api/v1/telemetry/central/cluster_deployments" % self.__cth_service)
            logger.debug(res.text)
            if res.status_code == 200 or res.status_code == 201:
                data = res.json()
        except Exception as e:
            logger.error("Unable to query Central Telemetry Handler for application deployments")
            logger.error(str(e))
        return data

    def __query_clusters_and_kernels(self):
        data = {}
        try:
            res = requests.get("%s/api/v1/telemetry/central/infrastructure?kernels=all" % self.__cth_service)
            logger.debug(res.text)
            if res.status_code == 200 or res.status_code == 201:
                data = res.json()
        except Exception as e:
            logger.error("Unable to query Central Telemetry Handler for available clusters and kernels")
            logger.error(str(e))
        return data

    def __query_on_demand_kernel_deployments(self):
        data = {}
        try:
            res = requests.get("%s/api/v1/telemetry/central/serrano_kernel_deployments?deployment_mode=FaaS" % self.__cth_service)
            logger.debug(res.text)
            if res.status_code == 200 or res.status_code == 201:
                data = res.json()
        except Exception as e:
            logger.error("Unable to query telemetry services for deployed on-demand kernels")
            logger.error(str(e))
        return data

    def __query_storage_locations(self):
        data = {}
        try:
            res = requests.get("%s/api/v1/telemetry/central/storage_locations" % self.__cth_service)
            logger.debug(res.text)
            if res.status_code == 200 or res.status_code == 201:
                data = res.json()
        except Exception as e:
            logger.error("Unable to query Central Telemetry Handler for available cloud storage locations")
            logger.error(str(e))
        return data
