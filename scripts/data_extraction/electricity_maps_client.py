import logging

import requests

logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO)

logger = logging.getLogger(__name__)


class ElectricityMapsClient:
    """
    Client for the ElectricityMap API.
    """

    def __init__(self, token, zone):
        """
        Initialize the client with an API token.
        """
        self.token = token
        self.base_url = "https://api.electricitymap.org/v3"
        self.headers = {"auth-token": self.token}
        self.zone = zone
        logger.info("ElectricityMaps Client initialized")

    def _make_request(self, endpoint: str) -> dict:
        """
        Make a request to the API.
        args:
            endpoint (str): The endpoint to query.
            params (dict): Query parameters.
        returns:
            dict: The response from the API.
        """
        url = f"{self.base_url}/{endpoint}/history?zone={self.zone}"
        try:
            response = requests.get(url, headers=self.headers, timeout=15)
        except Exception as e:
            logger.error(f"Request failed: {e}")
        return response.json()

    def get_carbon_intensity(self) -> dict:
        """
        Get the carbon intensity for a zone.
        returns:
            dict: The carbon intensity data.
        """
        return self._make_request("carbon-intensity")

    def get_power_breakdown(self) -> dict:
        """
        Get the power breakdown for a zone.
        returns:
            dict: The power breakdown data.
        """
        return self._make_request("power-breakdown")
