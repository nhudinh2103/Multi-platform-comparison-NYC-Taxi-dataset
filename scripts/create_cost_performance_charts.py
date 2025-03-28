import matplotlib.pyplot as plt
import numpy as np
import os
import pathlib

# Get the script directory and project root directory
SCRIPT_DIR = pathlib.Path(__file__).parent.absolute()
PROJECT_ROOT = SCRIPT_DIR.parent.absolute()

# Create images directories if they don't exist
os.makedirs(os.path.join(PROJECT_ROOT, 'images/comparison'), exist_ok=True)

# Cost comparison data (in USD)
# Azure costs
azure_storage_cost = 2.03  # daily storage cost (0.29 * 7 for 7-day time travel)
azure_copy_data_cost = 2.66  # data transfer cost per run (formerly network)
azure_compute_vm_cost = 1.88  # VM instance cost per run
azure_compute_databricks_cost = 11.10  # Databricks cluster cost per run
azure_transform_cost = 2.13  # Databricks SQL Warehouse cost per run

# GCP costs
gcp_storage_cost = 0.69  # daily storage cost (0.59 + 0.1)
gcp_copy_data_cost = 12.98  # data egress cost per run (formerly network)
gcp_compute_vm_cost = 2.60  # VM instance cost per run
gcp_compute_databricks_cost = 14.22  # Databricks cluster cost per run
gcp_transform_cost = 7.84  # BigQuery transform cost per run
gcp_transform_databricks_cost = 2.7  # Databricks SQL Warehouse cost per run

# Snowflake costs
snowflake_storage_cost = 1.031  # daily snowflake storage cost (0.59 + 0.441 for 7-day time travel)
snowflake_egress_gcp_cost = 6.37  # egress copy from GCP cost per run
snowflake_transform_cost = 38.0  # transform data cost per run


# Calculate total costs (including copy data/egress)
azure_total_with_copy_data_cost = azure_storage_cost + azure_copy_data_cost + azure_compute_vm_cost + azure_compute_databricks_cost
gcp_total_with_copy_data_cost = gcp_storage_cost + gcp_copy_data_cost + gcp_compute_vm_cost + gcp_compute_databricks_cost + gcp_transform_cost

# Calculate total costs (excluding copy data/egress as per requirement)
azure_total_cost = azure_storage_cost + azure_compute_vm_cost + azure_compute_databricks_cost
gcp_total_cost = gcp_storage_cost + gcp_compute_vm_cost + gcp_compute_databricks_cost + gcp_transform_cost

# Calculate transform costs for comparison
bigquery_transform_cost = gcp_transform_cost  # BigQuery transform cost
databricks_gcp_transform_cost = gcp_transform_databricks_cost  # Databricks SQL Warehouse cost on GCP
# snowflake_transform_cost already has the correct naming

# Performance comparison data (in minutes)
# Azure performance
azure_convert_time = 120.65  # Convert CSV to Parquet (120min 39s)
azure_transform_time = 11.47  # Transform (11min 28s)
azure_materialize_time = 14.43  # Materialize (14min 26s)

# GCP performance
gcp_convert_time = 91.0  # Convert CSV to Parquet (91min)
gcp_transform_time = 1.62  # Transform (1min 37s)
gcp_materialize_time = 1.05  # Materialize (1min 3s)

# Calculate total execution times
azure_total_time = azure_convert_time + azure_transform_time + azure_materialize_time
gcp_total_time = gcp_convert_time + gcp_transform_time + gcp_materialize_time

# Custom autopct function to show both percentage and value
def make_autopct(values):
    def my_autopct(pct):
        total = sum(values)
        val = pct * total / 100.0
        return '{p:.1f}%\n(${v:.2f})'.format(p=pct, v=val)
    return my_autopct

# Calculate total costs for GCP with Databricks SQL
gcp_total_databricks_cost = gcp_storage_cost + gcp_compute_vm_cost + gcp_compute_databricks_cost + gcp_transform_databricks_cost

# Create cost comparison pie charts (excluding network/egress costs) - side by side
plt.figure(figsize=(20, 8))

# First subplot - Azure vs GCP with BigQuery
plt.subplot(1, 2, 1)
labels_bigquery = ['Azure', 'GCP with BigQuery']
sizes_bigquery = [azure_total_cost, gcp_total_cost]
colors_bigquery = ['#EA4335', '#0078D4']  # Azure red, Google blue
explode_bigquery = (0.1, 0)  # explode Azure slice

plt.pie(sizes_bigquery, explode=explode_bigquery, labels=labels_bigquery, colors=colors_bigquery, 
        autopct=make_autopct(sizes_bigquery),
        shadow=True, startangle=140, textprops={'fontsize': 14})
plt.axis('equal')
plt.title('Cost Comparison: Azure vs GCP with BigQuery', fontsize=14)

# Second subplot - Azure vs GCP with Databricks SQL
plt.subplot(1, 2, 2)
labels_databricks = ['Azure', 'GCP with Databricks SQL']
sizes_databricks = [azure_total_cost, gcp_total_databricks_cost]
colors_databricks = ['#EA4335', '#0078D4']  # Azure red, Google blue
explode_databricks = (0.1, 0)  # explode Azure slice

plt.pie(sizes_databricks, explode=explode_databricks, labels=labels_databricks, colors=colors_databricks, 
        autopct=make_autopct(sizes_databricks),
        shadow=True, startangle=140, textprops={'fontsize': 14})
plt.axis('equal')
plt.title('Cost Comparison: Azure vs GCP with Databricks SQL', fontsize=14)

plt.tight_layout()
plt.savefig(os.path.join(PROJECT_ROOT, 'images/comparison/cost-comparison-pie.png'), dpi=300, bbox_inches='tight')
plt.close()

# Create detailed cost breakdown for Azure
plt.figure(figsize=(10, 6))
# Combine VM Instance and Databricks Cluster into Computing (Convert CSV to parquet)
azure_compute_convert_cost = azure_compute_vm_cost + azure_compute_databricks_cost
azure_labels = ['Storage', 'Copy Data', 'Computing (Convert CSV to parquet)', 'Transform (Databricks SQL Warehouse)']
azure_sizes = [azure_storage_cost, azure_copy_data_cost, azure_compute_convert_cost, azure_transform_cost]
azure_colors = ['#0078D4', '#50e6ff', '#243a5e', '#0063B1']

plt.pie(azure_sizes, labels=azure_labels, colors=azure_colors, 
        autopct=make_autopct(azure_sizes),
        shadow=True, startangle=140, textprops={'fontsize': 14})
plt.axis('equal')
plt.title('Azure Cost Breakdown\nTotal: $19.16/run + $2.03/day', fontsize=14)
plt.tight_layout()
plt.savefig(os.path.join(PROJECT_ROOT, 'images/comparison/azure-cost-breakdown.png'), dpi=300, bbox_inches='tight')
plt.close()

# Create detailed cost breakdown for GCP (with two options side by side)
plt.figure(figsize=(20, 8))

# First subplot - GCP with BigQuery
plt.subplot(1, 2, 1)
# Combine VM Instance and Databricks Cluster into Computing (Convert CSV to parquet)
gcp_compute_convert_cost = gcp_compute_vm_cost + gcp_compute_databricks_cost
gcp_bigquery_labels = ['Storage', 'Copy Data', 'Computing (Convert CSV to parquet)', 'Transform (BigQuery)']
gcp_bigquery_sizes = [gcp_storage_cost, gcp_copy_data_cost, gcp_compute_convert_cost, gcp_transform_cost]
gcp_bigquery_colors = ['#4285F4', '#34A853', '#FBBC05', '#5F6368']

plt.pie(gcp_bigquery_sizes, labels=gcp_bigquery_labels, colors=gcp_bigquery_colors, 
        autopct=make_autopct(gcp_bigquery_sizes),
        shadow=True, startangle=140, textprops={'fontsize': 12})
plt.axis('equal')
plt.title('GCP Cost Breakdown with BigQuery\nTotal: $38.31/run + $0.69/day', fontsize=14)

# Second subplot - GCP with Databricks SQL Warehouse
plt.subplot(1, 2, 2)
# Combine VM Instance and Databricks Cluster into Computing (Convert CSV to parquet)
gcp_compute_convert_cost = gcp_compute_vm_cost + gcp_compute_databricks_cost
gcp_databricks_labels = ['Storage', 'Copy Data', 'Computing (Convert CSV to parquet)', 'Transform (Databricks SQL Warehouse)']
gcp_databricks_sizes = [gcp_storage_cost, gcp_copy_data_cost, gcp_compute_convert_cost, gcp_transform_databricks_cost]
gcp_databricks_colors = ['#4285F4', '#34A853', '#FBBC05', '#5F6368']  # Using same color as BigQuery Transform

plt.pie(gcp_databricks_sizes, labels=gcp_databricks_labels, colors=gcp_databricks_colors, 
        autopct=make_autopct(gcp_databricks_sizes),
        shadow=True, startangle=140, textprops={'fontsize': 12})
plt.axis('equal')
plt.title('GCP Cost Breakdown with Databricks SQL\nTotal: $33.17/run + $0.69/day', fontsize=14)

plt.tight_layout()
plt.savefig(os.path.join(PROJECT_ROOT, 'images/comparison/gcp-cost-breakdown.png'), dpi=300, bbox_inches='tight')
plt.close()

# Create performance comparison bar chart (total time)
plt.figure(figsize=(15, 8))  # Increased width and height for more space
perf_labels = ['Azure', 'GCP']  # We only have performance data for GCP with BigQuery
x = np.arange(len(perf_labels))
width = 0.35

# Use more distinct colors for better differentiation
bar_colors = ['#EA4335', '#0078D4']  # Azure red, Google blue
bars = plt.bar(x, [azure_total_time, gcp_total_time], width, color=bar_colors)
plt.xlabel('Cloud Provider', fontsize=14)
plt.ylabel('Total Execution Time (Minutes)', fontsize=14)
plt.title('Performance Comparison: Azure vs GCP (Total Minutes)', fontsize=16)
plt.xticks(x, perf_labels, fontsize=12)
plt.yticks(fontsize=12)

# Set y-axis limit to leave more room for labels
max_value = max(azure_total_time, gcp_total_time)
plt.ylim(0, max_value * 1.2)  # Add 20% more space at the top

# Add value labels on top of bars
for i, v in enumerate([azure_total_time, gcp_total_time]):
    plt.text(i, v + (max_value * 0.03), f"{v:.2f}", ha='center', fontsize=12)

plt.tight_layout()
plt.savefig(os.path.join(PROJECT_ROOT, 'images/comparison/performance-comparison-bar.png'), dpi=300, bbox_inches='tight')
plt.close()

# Create detailed performance breakdown bar chart
plt.figure(figsize=(12, 9))  # Increased height for more space
x = np.arange(3)  # 3 operations
width = 0.35

# Azure bars - using a distinct red
azure_bars = plt.bar(x - width/2, [azure_convert_time, azure_transform_time, azure_materialize_time], 
                    width, label='Azure', color='#EA4335')

# GCP bars - using Google blue for better contrast
gcp_bars = plt.bar(x + width/2, [gcp_convert_time, gcp_transform_time, gcp_materialize_time], 
                  width, label='GCP', color='#0078D4')

plt.xlabel('Processing Step', fontsize=14)
plt.ylabel('Execution Time (Minutes)', fontsize=14)
plt.title('Performance Breakdown by Processing Step', fontsize=16)
plt.xticks(x, ['Convert CSV to Parquet', 'Transform', 'Materialize'], fontsize=12)
plt.yticks(fontsize=12)
plt.legend(fontsize=12)

# Set y-axis limit to leave more room for labels
max_value = max(azure_convert_time, azure_transform_time, azure_materialize_time, 
                gcp_convert_time, gcp_transform_time, gcp_materialize_time)
plt.ylim(0, max_value * 1.15)  # Add 15% more space at the top

# Add value labels on top of bars
for i, bars in enumerate([azure_bars, gcp_bars]):
    for bar in bars:
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2., height + (max_value * 0.02),
                f"{height:.2f}", ha='center', va='bottom', fontsize=10)

plt.tight_layout()
plt.savefig(os.path.join(PROJECT_ROOT, 'images/comparison/performance-breakdown-bar.png'), dpi=300, bbox_inches='tight')
plt.close()

# Create a zoomed-in version for transform and materialize steps (excluding the convert step)
plt.figure(figsize=(10, 7))  # Increased height for more space
x = np.arange(2)  # 2 operations (transform and materialize)
width = 0.35

# Azure bars - using a distinct red
azure_bars = plt.bar(x - width/2, [azure_transform_time, azure_materialize_time], 
                    width, label='Azure', color='#EA4335')

# GCP bars - using Google blue for better contrast
gcp_bars = plt.bar(x + width/2, [gcp_transform_time, gcp_materialize_time], 
                  width, label='GCP', color='#0078D4')

plt.xlabel('Processing Step', fontsize=14)
plt.ylabel('Execution Time (Minutes)', fontsize=14)
plt.title('Performance Comparison: Transform and Materialize Steps', fontsize=16)
plt.xticks(x, ['Transform', 'Materialize'], fontsize=12)
plt.yticks(fontsize=12)
plt.legend(fontsize=12)

# Set y-axis limit to leave more room for labels
max_value = max(azure_transform_time, azure_materialize_time, gcp_transform_time, gcp_materialize_time)
plt.ylim(0, max_value * 1.3)  # Add 30% more space at the top for this chart

# Add value labels on top of bars
for i, bars in enumerate([azure_bars, gcp_bars]):
    for bar in bars:
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2., height + (max_value * 0.05),
                f"{height:.2f}", ha='center', va='bottom', fontsize=10)

plt.tight_layout()
plt.savefig(os.path.join(PROJECT_ROOT, 'images/comparison/transform-materialize-comparison.png'), dpi=300, bbox_inches='tight')
plt.close()

# Create Transform Cost Comparison chart (BigQuery vs Databricks vs Snowflake)
plt.figure(figsize=(12, 8))
x = np.arange(3)  # 3 platforms
width = 0.5

# Use distinct colors
platform_colors = ['#4285F4', '#EA4335', '#29B5E8']  # BigQuery blue, Databricks red, Snowflake blue
transform_costs = [bigquery_transform_cost, databricks_gcp_transform_cost, snowflake_transform_cost]
bars = plt.bar(x, transform_costs, width, color=platform_colors)
plt.xlabel('Platform', fontsize=14)
plt.ylabel('Transform Cost (USD per Run)', fontsize=14)
plt.title('Transform Cost Comparison: BigQuery vs Databricks vs Snowflake', fontsize=16)
plt.xticks(x, ['BigQuery', 'Databricks', 'Snowflake'], fontsize=12)
plt.yticks(fontsize=12)

# Set y-axis limit to leave more room for labels
max_value = max(transform_costs)
plt.ylim(0, max_value * 1.2)  # Add 20% more space at the top

# Add value labels on top of bars
for i, v in enumerate(transform_costs):
    plt.text(i, v + (max_value * 0.03), f"${v:.2f}", ha='center', fontsize=12)

plt.tight_layout()
plt.savefig(os.path.join(PROJECT_ROOT, 'images/comparison/transform-cost-comparison.png'), dpi=300, bbox_inches='tight')
plt.close()

print(f"All charts have been generated and saved to '{os.path.join(PROJECT_ROOT, 'images/comparison')}' directory.")
