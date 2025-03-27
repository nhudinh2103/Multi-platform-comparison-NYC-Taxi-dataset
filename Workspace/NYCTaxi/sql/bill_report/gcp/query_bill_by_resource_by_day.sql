-- First identify which service/resource combinations have at least one row with a matching label key
WITH matching_resources AS (
  SELECT DISTINCT
    service.description AS service_description,
    resource.name AS resource_name,
    resource.global_name AS resource_global_name
  FROM
    `amplified-brook-454012-i1.all_billing_data.gcp_billing_export_resource_v1_01B031_67DCCD_1223E8`,
    UNNEST(labels) AS label
  -- WHERE
    -- label_key_filter = '' OR label.key = label_key_filter
),

-- Pre-aggregate labels for each resource to avoid multiple rows per resource
resource_labels AS (
  SELECT
    DATE(usage_start_time) AS usage_date,
    service.description AS service_description,
    resource.name AS resource_name,
    resource.global_name AS resource_global_name,
    -- Aggregate all labels for this resource into a single string
    STRING_AGG(CONCAT(label.key, ',', label.value), ';') AS labels_string
  FROM
    `amplified-brook-454012-i1.all_billing_data.gcp_billing_export_resource_v1_01B031_67DCCD_1223E8`,
    UNNEST(labels) AS label
  GROUP BY
    usage_date,
    service_description,
    resource_name,
    resource_global_name
),

-- Then get all billing data for those combinations
billing_data AS (
  SELECT
    -- Extract date from usage start time
    DATE(billing.usage_start_time) AS usage_date,
    
    -- Service and resource information
    billing.service.description AS service_description,
    billing.resource.name AS resource_name,
    billing.resource.global_name AS resource_global_name,
    
    -- Get pre-aggregated labels
    rl.labels_string,
    
    -- Cost in local currency (cost with credits/adjustments applied)
    SUM(billing.cost) AS local_currency_cost,
    
    -- Cost in USD (converting if needed)
    SUM(
      -- If currency is already USD, use cost directly
      -- Otherwise convert from local currency to USD by dividing by the conversion rate
      IF(billing.currency = 'USD', 
         billing.cost, 
         billing.cost / billing.currency_conversion_rate)
    ) AS usd_cost,
    
    -- Include currency code for reference
    billing.currency AS local_currency
  FROM
    `amplified-brook-454012-i1.all_billing_data.gcp_billing_export_resource_v1_01B031_67DCCD_1223E8` billing
  -- Join to the matching resources to filter
  LEFT JOIN matching_resources mr
    ON billing.service.description = mr.service_description
    AND billing.resource.name = mr.resource_name
    AND billing.resource.global_name = mr.resource_global_name
  -- Join to get pre-aggregated labels
  LEFT JOIN resource_labels rl
    ON DATE(billing.usage_start_time) = rl.usage_date
    AND billing.service.description = rl.service_description
    AND billing.resource.name = rl.resource_name
    AND billing.resource.global_name = rl.resource_global_name
  GROUP BY
    usage_date, 
    service_description, 
    resource_name, 
    resource_global_name,
    labels_string,
    currency
)

SELECT
  usage_date,
  service_description,
  resource_name,
  resource_global_name,
  labels_string,
  -- Format local currency with 2 decimal places
  ROUND(local_currency_cost, 2) AS local_currency_cost,
  local_currency,
  -- Format USD with 2 decimal places
  ROUND(usd_cost, 2) AS usd_cost,
  'USD' AS usd_currency
FROM
  billing_data
-- Order by date (newest first) and then by cost (highest first)
ORDER BY
  usage_date DESC,
  usd_cost DESC;