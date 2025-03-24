import matplotlib.pyplot as plt
import numpy as np
import os

# Create images directories if they don't exist
os.makedirs('images/comparison', exist_ok=True)

# Cost comparison data (in USD)
# Azure costs
azure_storage = 1.39  # daily storage cost
azure_network = 2.66  # data transfer cost per run
azure_compute_vm = 1.88  # VM instance cost per run
azure_compute_databricks = 11.10  # Databricks cluster cost per run
azure_transform = 0  # Placeholder for Databricks SQL Warehouse (updating)

# GCP costs
gcp_storage = 0.67  # daily storage cost
gcp_network = 12.98  # data egress cost per run
gcp_compute_vm = 2.60  # VM instance cost per run
gcp_compute_databricks = 14.22  # Databricks cluster cost per run
gcp_transform = 0
# gcp_transform = 7.84  # BigQuery transform cost per run

# Calculate total costs (including network/egress)
azure_total_with_network = azure_storage + azure_network + azure_compute_vm + azure_compute_databricks
gcp_total_with_network = gcp_storage + gcp_network + gcp_compute_vm + gcp_compute_databricks + gcp_transform

# Calculate total costs (excluding network/egress as per requirement)
azure_total = azure_storage + azure_compute_vm + azure_compute_databricks
gcp_total = gcp_storage + gcp_compute_vm + gcp_compute_databricks + gcp_transform

# Performance comparison data (in minutes)
# Azure performance
azure_convert = 120.65  # Convert CSV to Parquet (120min 39s)
azure_transform = 11.47  # Transform (11min 28s)
azure_materialize = 14.43  # Materialize (14min 26s)

# GCP performance
gcp_convert = 91.0  # Convert CSV to Parquet (91min)
gcp_transform = 1.62  # Transform (1min 37s)
gcp_materialize = 1.05  # Materialize (1min 3s)

# Calculate total execution times
azure_total_time = azure_convert + azure_transform + azure_materialize
gcp_total_time = gcp_convert + gcp_transform + gcp_materialize

# Create cost comparison pie chart (excluding network/egress costs)
plt.figure(figsize=(10, 6))
labels = ['Azure', 'GCP']
sizes = [azure_total, gcp_total]
colors = ['#0078D4', '#EA4335']  # Azure blue, Google red
explode = (0.1, 0)  # explode Azure slice

plt.pie(sizes, explode=explode, labels=labels, colors=colors, autopct='%1.1f%%',
        shadow=True, startangle=140, textprops={'fontsize': 14})
plt.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle
plt.title('Cost Comparison: Azure vs GCP (Total USD per Run, Excluding Network Costs)', fontsize=16)
plt.tight_layout()
plt.savefig('images/comparison/cost-comparison-pie.png', dpi=300, bbox_inches='tight')
plt.close()

# Create detailed cost breakdown for Azure
plt.figure(figsize=(10, 6))
azure_labels = ['Storage', 'Network', 'VM Instance', 'Databricks Cluster']
azure_sizes = [azure_storage, azure_network, azure_compute_vm, azure_compute_databricks]
azure_colors = ['#0078D4', '#50e6ff', '#81b0d2', '#243a5e']

plt.pie(azure_sizes, labels=azure_labels, colors=azure_colors, autopct='%1.1f%%',
        shadow=True, startangle=140, textprops={'fontsize': 14})
plt.axis('equal')
plt.title('Azure Cost Breakdown (USD per Run)', fontsize=16)
plt.tight_layout()
plt.savefig('images/comparison/azure-cost-breakdown.png', dpi=300, bbox_inches='tight')
plt.close()

# Create detailed cost breakdown for GCP
plt.figure(figsize=(10, 6))
gcp_labels = ['Storage', 'Network Egress', 'VM Instance', 'Databricks Cluster', 'BigQuery Transform']
gcp_sizes = [gcp_storage, gcp_network, gcp_compute_vm, gcp_compute_databricks, gcp_transform]
gcp_colors = ['#4285F4', '#34A853', '#FBBC05', '#EA4335', '#5F6368']

plt.pie(gcp_sizes, labels=gcp_labels, colors=gcp_colors, autopct='%1.1f%%',
        shadow=True, startangle=140, textprops={'fontsize': 14})
plt.axis('equal')
plt.title('GCP Cost Breakdown (USD per Run)', fontsize=16)
plt.tight_layout()
plt.savefig('images/comparison/gcp-cost-breakdown.png', dpi=300, bbox_inches='tight')
plt.close()

# Create performance comparison bar chart (total time)
plt.figure(figsize=(10, 6))
x = np.arange(len(labels))
width = 0.35

# Use more distinct colors for better differentiation
bar_colors = ['#0078D4', '#EA4335']  # Azure blue, Google red
plt.bar(x, [azure_total_time, gcp_total_time], width, color=bar_colors)
plt.xlabel('Cloud Provider', fontsize=14)
plt.ylabel('Total Execution Time (Minutes)', fontsize=14)
plt.title('Performance Comparison: Azure vs GCP (Total Minutes)', fontsize=16)
plt.xticks(x, labels, fontsize=12)
plt.yticks(fontsize=12)

# Add value labels on top of bars
for i, v in enumerate([azure_total_time, gcp_total_time]):
    plt.text(i, v + 5, f"{v:.2f}", ha='center', fontsize=12)

plt.tight_layout()
plt.savefig('images/comparison/performance-comparison-bar.png', dpi=300, bbox_inches='tight')
plt.close()

# Create detailed performance breakdown bar chart
plt.figure(figsize=(12, 7))
x = np.arange(3)  # 3 operations
width = 0.35

# Azure bars - using a distinct blue
azure_bars = plt.bar(x - width/2, [azure_convert, azure_transform, azure_materialize], 
                    width, label='Azure', color='#0078D4')

# GCP bars - using Google red for better contrast
gcp_bars = plt.bar(x + width/2, [gcp_convert, gcp_transform, gcp_materialize], 
                  width, label='GCP', color='#EA4335')

plt.xlabel('Processing Step', fontsize=14)
plt.ylabel('Execution Time (Minutes)', fontsize=14)
plt.title('Performance Breakdown by Processing Step', fontsize=16)
plt.xticks(x, ['Convert CSV to Parquet', 'Transform', 'Materialize'], fontsize=12)
plt.yticks(fontsize=12)
plt.legend(fontsize=12)

# Add value labels on top of bars
for i, bars in enumerate([azure_bars, gcp_bars]):
    for bar in bars:
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2., height + 2,
                f"{height:.2f}", ha='center', va='bottom', fontsize=10)

plt.tight_layout()
plt.savefig('images/comparison/performance-breakdown-bar.png', dpi=300, bbox_inches='tight')
plt.close()

# Create a zoomed-in version for transform and materialize steps (excluding the convert step)
plt.figure(figsize=(10, 6))
x = np.arange(2)  # 2 operations (transform and materialize)
width = 0.35

# Azure bars - using a distinct blue
azure_bars = plt.bar(x - width/2, [azure_transform, azure_materialize], 
                    width, label='Azure', color='#0078D4')

# GCP bars - using Google red for better contrast
gcp_bars = plt.bar(x + width/2, [gcp_transform, gcp_materialize], 
                  width, label='GCP', color='#EA4335')

plt.xlabel('Processing Step', fontsize=14)
plt.ylabel('Execution Time (Minutes)', fontsize=14)
plt.title('Performance Comparison: Transform and Materialize Steps', fontsize=16)
plt.xticks(x, ['Transform', 'Materialize'], fontsize=12)
plt.yticks(fontsize=12)
plt.legend(fontsize=12)

# Add value labels on top of bars
for i, bars in enumerate([azure_bars, gcp_bars]):
    for bar in bars:
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2., height + 0.5,
                f"{height:.2f}", ha='center', va='bottom', fontsize=10)

plt.tight_layout()
plt.savefig('images/comparison/transform-materialize-comparison.png', dpi=300, bbox_inches='tight')
plt.close()

print("All charts have been generated and saved to the 'images/comparison' directory.")
