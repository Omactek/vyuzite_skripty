import pandas as pd

old = r'D:\School\bakalarka\data\data_wip\02-1hod_checked\Cikansky_potok_old_hour_final.csv'
new = r'D:\School\bakalarka\data\data_wip\02-1hod_checked\Cikansky_potok_hour_final.csv'
merged = r'D:\School\bakalarka\data\data_wip\02-1hod_checked\Cikansky_potok_merged_hour_final.csv'

old_df = pd.read_csv(old, sep = ';')

new_df = pd.read_csv(new, sep = ';')

merged_df = old_df.merge(new_df, how='outer')

merged_df.to_csv(merged, sep=';', na_rep='NA')
