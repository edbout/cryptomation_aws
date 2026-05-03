from py_clob_client_v2.client import ClobClient
from py_clob_client_v2.clob_types import ApiCreds
import os
import logging
from typing import Optional, Dict

logger = logging.getLogger(__name__)


class ClobClientFactory:
    """Factory for Polymarket V2 ClobClient with auth fallback."""

    def __init__(self, host: str = "https://clob.polymarket.com", chain_id: int = 137) -> None:
        self.host = host
        self.chain_id = chain_id

    def create_client(
        self,
        private_key: Optional[str] = None,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
        api_passphrase: Optional[str] = None,
    ) -> ClobClient:
        key = private_key or os.getenv("PRIVATE_KEY")
        if not key:
            raise ValueError("Missing private key. Set PRIVATE_KEY or pass as argument.")

        client = ClobClient(
            host=self.host,
            key=key,
            chain_id=self.chain_id,
            signature_type=0,  # EOA: signer == maker
        )

        signer_addr = client.get_address()
        logger.info("🔑 create_client | signer=%s… | sig_type=0 (EOA)", signer_addr[:10] if signer_addr else "?")

        try:
            logger.info("🔑 create_client | Attempting automatic API credential derivation...")
            api_creds = client.create_or_derive_api_key()
            client.set_api_creds(api_creds)
            logger.info("✓ create_client | Successfully derived API credentials automatically.")
            return client

        except Exception as derive_error:
            logger.warning(f"⚠️ create_client | Automatic credential derivation failed: {derive_error}")

        # Fallback: manual API credentials from arguments or environment
        manual_key = api_key or os.getenv("API_KEY")
        manual_secret = api_secret or os.getenv("API_SECRET")
        manual_passphrase = api_passphrase or os.getenv("API_PASSPHRASE")

        if all([manual_key, manual_secret, manual_passphrase]):
            client.set_api_creds(ApiCreds(
                api_key=manual_key,
                api_secret=manual_secret,
                api_passphrase=manual_passphrase,
            ))
            logger.info("✓ create_client | Loaded manual API credentials successfully.")
            return client

        raise ValueError(
            "Authentication failed: could not auto‑derive or find manual credentials.\n"
            "💡 To fix: ensure PRIVATE_KEY or API_KEY/API_SECRET/API_PASSPHRASE env vars are set."
        )

    @classmethod
    def from_env(cls) -> "ClobClientFactory":
        return cls()

    @staticmethod
    def get_default_config() -> Dict[str, int]:
        return {
            "host": "https://clob.polymarket.com",
            "chain_id": 137,
            "signature_type": 0,
        }
