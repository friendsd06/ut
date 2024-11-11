import os
import shutil
from datetime import datetime
from ...core.ports.interfaces import StorageService


class LocalStorage(StorageService):
    def save(self, path: str, content: str) -> None:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, 'w') as f:
            f.write(content)

    def exists(self, path: str) -> bool:
        return os.path.exists(path)

    def backup(self, path: str) -> None:
        if self.exists(path):
            backup_path = f"{path}.{datetime.now().strftime('%Y%m%d_%H%M%S')}.bak"
            shutil.copy2(path, backup_path)
