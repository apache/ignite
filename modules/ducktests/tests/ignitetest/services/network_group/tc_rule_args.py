import socket
from dataclasses import dataclass
from typing import Optional

from ignitetest.services.network_group.configuration import CrossNetworkGroupConfiguration


@dataclass(frozen=True)
class TcRuleArgs:
    """
    Builds command-line arguments for tcconfig utilities.
    """
    interface: str
    dst_host_or_ip: str
    config: CrossNetworkGroupConfiguration

    def to_tcset_cmd(self, action: str = "--overwrite") -> Optional[str]:
        """
        Compiles the full 'tcset' command string.
        Returns None if there are no network limitations configured.
        """
        if not self.config.delay and self.config.loss is None:
            return None

        dst_ip = socket.gethostbyname(self.dst_host_or_ip)

        args = [
            f"sudo tcset {self.interface}",
            f"--dst-network {dst_ip}/32",
            action
        ]

        if self.config.delay:
            args.append(f"--delay {self.config.delay}")

        if self.config.loss is not None:
            args.append(f"--loss {self.config.loss * 100}%")

        return " ".join(args)

    @classmethod
    def to_tcdel_all_cmd(cls, interface: str) -> str:
        """
        Compiles the absolute clear command for an interface.
        """
        return f"sudo tcdel {interface} --all || true"
