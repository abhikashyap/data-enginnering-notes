# pip install delta-sharing

import delta_sharing
profile = "/Users/shashankmishra/Downloads/config.share"
client = delta_sharing.SharingClient(profile)
print(client.list_all_tables())

# Output - [Table(name='high_risk_attrition_employees', share='dbx-data-assests', schema='default'), Table(name='employee_attrition', share='dbx-data-assests', schema='default')]

shareName="dbx-data-assests"
schemaName="default"
tableName="high_risk_attrition_employees"
tblurl = f"{profile}#{shareName}.{schemaName}.{tableName}"
df = delta_sharing.load_as_pandas(tblurl)
print(df)