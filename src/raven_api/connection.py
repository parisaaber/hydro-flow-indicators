from typing import Union, Optional, List
import duckdb


class Connection:
    parquet_view: Union[None, duckdb.DuckDBPyRelation] = None

    def __init__(self):
        self.con = duckdb.connect(database=":memory:")
        """
        Initialize the connection to the DuckDB in-memory database.

        This is used as the default connection for all queries.
        """
        self._parquet_path: Optional[str] = None
        self._sites: Optional[List[str]] = None
        self._start_date: Optional[str] = None
        self._end_date: Optional[str] = None
        self._views_created = set()  # Track created views

    def configure(
        self,
        parquet_path: Optional[str] = None,
        sites: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ):
        """Configure the connection with commonly used parameters"""
        if parquet_path:
            self._parquet_path = parquet_path
            # Create a main data view when parquet path is set
            self.create_main_data_view()

        if sites is not None:
            self._sites = sites

        if start_date:
            self._start_date = start_date

        if end_date:
            self._end_date = end_date
        self.create_filtered_data_view()

    def create_main_data_view(self):
        if not self._parquet_path:
            raise ValueError("Parquet path must be configured first")

        view_name = "main_data"
        if view_name not in self._views_created:
            query = f"""
                CREATE OR REPLACE VIEW {view_name} AS
                SELECT*
                FROM parquet_scan('{self._parquet_path}')
            """
            self.con.execute(query)
            self._views_created.add(view_name)

        return view_name

    def create_filtered_data_view(self, view_name: str = "filtered_data"):
        """
        Create a filtered data view with optional site/date filters.
        """
        # Ensure main_data view exists first
        if "main_data" not in self._views_created:
            if not self._parquet_path:
                raise ValueError("Parquet path must be configured first")
            self.create_main_data_view()

        filters = []
        if self._sites:
            # Properly escape and quote site names
            quoted_sites = (
                [f"'{site.replace("'", "''")}'" for site in self._sites]
                )
            if len(quoted_sites) == 1:
                filters.append(f"site = {quoted_sites[0]}")
            else:
                sites_list = ", ".join(quoted_sites)
                filters.append(f"site IN ({sites_list})")
        if self._start_date and self._end_date:
            filters.append(
                f"date BETWEEN '{self._start_date}' AND '{self._end_date}'"
                )

        where_clause = " AND ".join(filters) if filters else "1=1"

        self.con.execute(f"""
            CREATE OR REPLACE VIEW {view_name} AS
            SELECT *
            FROM main_data
            WHERE {where_clause}
        """)

        self._views_created.add(view_name)
        return view_name

    def create_common_views(self):
        """Create commonly used aggregated views for performance"""
        # Ensure main_data view exists first
        if "main_data" not in self._views_created:
            if not self._parquet_path:
                raise ValueError("Parquet path must be configured first")
            self.create_main_data_view()

        # Mean annual flow per site view -
        maf_view = """
            CREATE OR REPLACE VIEW mean_annual_flows AS
            WITH yearly_means AS (
                SELECT site, water_year, AVG(value) AS maf_per_year
                FROM main_data
                WHERE value IS NOT NULL
                GROUP BY site, water_year
            )
            SELECT site, AVG(maf_per_year) AS mean_annual_flow
            FROM yearly_means
            GROUP BY site
        """
        self.con.execute(maf_view)
        self._views_created.add("mean_annual_flows")

        # Annual maximums view - SIMPLIFIED: main_data now has water_year
        annual_max_view = """
            CREATE OR REPLACE VIEW annual_maximums AS
            SELECT site, water_year, MAX(value) as annual_max
            FROM main_data
            WHERE value IS NOT NULL
            GROUP BY site, water_year
        """
        self.con.execute(annual_max_view)
        self._views_created.add("annual_maximums")

        # Daily statistics view
        daily_stats_view = """
            CREATE OR REPLACE VIEW daily_statistics AS
            SELECT
                site,
                date,
                value,
                water_year,
                EXTRACT(month FROM date) as month,
                EXTRACT(dayofyear FROM date) as day_of_year
            FROM main_data
            WHERE value IS NOT NULL
        """
        self.con.execute(daily_stats_view)
        self._views_created.add("daily_statistics")

    def execute(self, query: str):
        """Execute query on the connection"""
        return self.con.execute(query)

    def get_available_views(self) -> List[str]:
        """Get list of available views"""
        return list(self._views_created)

    def get_config(self) -> dict:
        """Get current configuration"""
        return {
            "parquet_path": self._parquet_path,
            "sites": self._sites,
            "start_date": self._start_date,
            "end_date": self._end_date,
            "available_views": list(self._views_created),
        }

    def reset_config(self):
        """Reset configuration and drop created views"""
        for view in self._views_created:
            try:
                self.con.execute(f"DROP VIEW IF EXISTS {view}")
            except Exception:
                pass  # View might not exist

        self._views_created.clear()
        self._parquet_path = None
        self._sites = None
        self._start_date = None
        self._end_date = None

    def __del__(self):
        if hasattr(self, "con"):
            self.con.close()
