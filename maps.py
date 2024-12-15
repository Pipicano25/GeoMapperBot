import pandas as pd

df_maps = pd.read_excel("archivo_completo.xlsx")
ss = []
for i in df_maps['usuario']:
    ss.append(i)


print(ss)