import duckdb
import hashlib
from collections import OrderedDict


class Connection:
    def __init__(self, max_cached_parquets: int = 12):
        self.con = duckdb.connect(database=":memory:")

        # Install and load httpfs
        try:
            self.con.install_extension("httpfs")
        except Exception:
            self.con.execute("INSTALL httpfs;")
        try:
            self.con.load_extension("httpfs")
        except Exception:
            self.con.execute("LOAD httpfs;")

        self._dataset_views: "OrderedDict[str, str]" = (
            OrderedDict()
        )  # parquet_path -> view_name
        self._max_cached = max_cached_parquets

    def _view_name_for(self, parquet_path: str) -> str:
        # Stable identifier per path
        h = hashlib.sha1(parquet_path.encode("utf-8")).hexdigest()[:12]
        return f"ds_{h}_main"

    def get_or_create_main_view(self, parquet_path: str) -> str:
        if parquet_path in self._dataset_views:
            view_name = self._dataset_views.pop(parquet_path)
            self._dataset_views[parquet_path] = view_name
            return view_name

        view_name = self._view_name_for(parquet_path)
        # Create/replace the view for this dataset
        esc = parquet_path.replace("'", "''")
        self.con.execute(
            f"""
            CREATE OR REPLACE VIEW {view_name} AS
            SELECT * FROM parquet_scan('{esc}')
        """
        )

        # cache insert
        self._dataset_views[parquet_path] = view_name
        if len(self._dataset_views) > self._max_cached:
            # evict oldest
            old_path, old_view = self._dataset_views.popitem(last=False)
            try:
                self.con.execute(f"DROP VIEW IF EXISTS {old_view}")
            except Exception:
                pass
        return view_name

    def execute(self, query: str):
        return self.con.execute(query)

    def reset(self):
        # Drop all cached views
        for _, v in list(self._dataset_views.items()):
            try:
                self.con.execute(f"DROP VIEW IF EXISTS {v}")
            except Exception:
                pass
        self._dataset_views.clear()

    def __del__(self):
        try:
            self.con.close()
        except Exception:
            pass
