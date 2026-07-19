# Meteorological Data Lakehouse: Data Dictionary

## Silver Layer: `silver_weather` (Core Fact Table)
*Note: All physical metrics in the Silver layer are stored as strings to preserve raw API formatting, requiring engine-side casting during Gold layer aggregations.*

| Column Name | Data Type | Description |
| :--- | :--- | :--- |
| `summary` | String | General weather condition description. |
| `temperature_2m` | String | Ambient air temperature at 2 meters above ground (°C). |
| `relative_humidity_2m` | String | Relative humidity percentage (0-100%). |
| `cloud_cover_mid` | String | Percentage of sky covered by mid-altitude clouds. |
| `cloud_cover_high` | String | Percentage of sky covered by high-altitude clouds. |
| `cloud_cover_low` | String | Percentage of sky covered by low-altitude clouds. |
| `rain` | String | Rain volume in the preceding hour (mm). |
| `snowfall` | String | Snowfall volume in the preceding hour (cm). |
| `precipitation` | String | Total precipitation (mm). |
| `dew_point_2m` | String | Dew point temperature at 2 meters above ground (°C). |
| `wind_speed_100m` | String | Wind speed measured at 100 meters above ground (km/h). |
| `wind_speed_10m` | String | Wind speed measured at 10 meters above ground (km/h). |
| `wind_direction_10m` | String | Wind direction at 10 meters above ground. |
| `file_name` | String | Source JSON/CSV file name. |
| `file_size` | String | Source file size. |
| `weather` | String | Categorical weather status. |
| `year` | String | Year of observation. |
| `month` | String | Month of observation. |
| `hour` | String | Hour of observation (0-23). |
| `five_year_partition` | String | Engineered partition key clustering data into 5-year groups. |

## Gold Layer: `gold_cloud_view_summary`
| Column Name | Data Type | Description |
| :--- | :--- | :--- |
| `year` | String | Year of the summary. |
| `month` | String | Month of the summary. |
| `weather` | String | Weather category. |
| `perfect_viewing_hours` | Long | Total hours matching all 5 "Sea of Clouds" constraints (index = 5). |
| `avg_low_cloud_pct` | Double | Average low cloud cover percentage. |
| `avg_humidity_pct` | Double | Average relative humidity percentage. |
| `avg_wind_speed_kmh` | Double | Average wind speed (km/h). |
