import pandas as pd

# Troque pelo nome do seu arquivo
df = pd.read_csv("Remanejamentos 2025-08-utf8.csv", sep=";")

# limpeza mínima
df = df.copy()
df["OD"] = df["OD"].astype(str).str.strip().str.upper()
df["CODACAO"] = df["CODACAO"].astype(str).str.strip()

# preserva a ordem original como critério secundário de sort
df["__idx"] = range(len(df))

# 2) separar origens e destinos
df_origens = df[df["OD"] == "O"].copy()
df_destinos = df[df["OD"] == "D"].copy()

# 3) "agrupar" apenas juntando por CODACAO (sem agregar/somar)
df_origens = (
    df_origens.sort_values(["CODACAO", "__idx"])
    .drop(columns="__idx")
    .reset_index(drop=True)
)
df_destinos = (
    df_destinos.sort_values(["CODACAO", "__idx"])
    .drop(columns="__idx")
    .reset_index(drop=True)
)

print("Origens:", len(df_origens), " | Destinos:", len(df_destinos))

# Guarde bases ANTES do filtro dos comuns
df_origens_base = df_origens.copy()
df_destinos_base = df_destinos.copy()

# Conjuntos de CODACAO de cada lado
cod_o = set(df_origens_base["CODACAO"])
cod_d = set(df_destinos_base["CODACAO"])

# Diferenças (o “contrário” da interseção)
cod_somente_origem = cod_o - cod_d  # aparecem só nas ORIGENS
cod_somente_destino = cod_d - cod_o  # aparecem só nos DESTINOS

# Filtra as linhas “sem par” em cada lado
df_only_o = df_origens_base[df_origens_base["CODACAO"].isin(cod_somente_origem)].copy()
df_only_d = df_destinos_base[
    df_destinos_base["CODACAO"].isin(cod_somente_destino)
].copy()

# pega a interseção dos CODACAO nos dois dataframes
codacoes_comuns = set(df_origens_base["CODACAO"]).intersection(
    df_destinos_base["CODACAO"]
)

# filtra os dois dfs mantendo só os CODACAO que aparecem nos dois
df_origens = df_origens_base[
    df_origens_base["CODACAO"].isin(codacoes_comuns)
].reset_index(drop=True)
df_destinos = df_destinos_base[
    df_destinos_base["CODACAO"].isin(codacoes_comuns)
].reset_index(drop=True)

codacoes_unicas = sorted(df["CODACAO"].unique().tolist())


# bases imutáveis para o loop
# df_origens_base  = df_origens.copy()
# df_destinos_base = df_destinos.copy()

df_RO = df_origens.iloc[0:0].copy()  # acumulador
df_CA = df_origens.iloc[0:0].copy()  # “sobra”

for acao in codacoes_unicas:
    # recortes só desta CODACAO (sem afetar as bases)
    df_o = df_origens[df_origens["CODACAO"] == acao].copy().reset_index(drop=True)
    df_d = df_destinos[df_destinos["CODACAO"] == acao].copy().reset_index(drop=True)

    soma_origens = pd.to_numeric(df_o["VALOR"], errors="coerce").fillna(0).sum()
    soma_destinos = pd.to_numeric(df_d["VALOR"], errors="coerce").fillna(0).sum()

    if soma_origens <= soma_destinos:
        # 1) joga todas as origens para o RO (vamos casar com destinos)
        df_RO = pd.concat([df_RO, df_o], ignore_index=True)

        diff_origem = soma_origens  # quanto ainda falta "fechar" de origem
        for pos, row in df_d.iterrows():
            val_dest = float(row["VALOR"])

            if val_dest > diff_origem:
                # divide a linha de destino: uma parte vai pro RO (fechando a origem),
                # a sobra vai pro CA, e o RESTANTE das linhas também vão pro CA
                parte_RO = df_d.iloc[[pos]].copy()
                parte_RO["VALOR"] = diff_origem
                df_RO = pd.concat([df_RO, parte_RO], ignore_index=True)

                sobra = df_d.iloc[[pos]].copy()
                sobra["VALOR"] = val_dest - diff_origem
                df_CA = pd.concat([df_CA, sobra], ignore_index=True)

                # tudo que vem depois também é CA
                if pos + 1 < len(df_d):
                    df_CA = pd.concat([df_CA, df_d.iloc[pos + 1 :]], ignore_index=True)
                break

            elif val_dest == diff_origem:
                # linha de destino fecha exatamente
                df_RO = pd.concat([df_RO, df_d.iloc[[pos]]], ignore_index=True)

                # o resto vai para CA
                if pos + 1 < len(df_d):
                    df_CA = pd.concat([df_CA, df_d.iloc[pos + 1 :]], ignore_index=True)
                break

            else:
                # val_dest < diff_origem -> inteiro para RO e continua
                df_RO = pd.concat([df_RO, df_d.iloc[[pos]]], ignore_index=True)
                diff_origem -= val_dest

        # se por algum motivo percorreu todos os destinos sem “break”, não sobrou nada a mandar pra CA aqui

    else:
        # soma_origens > soma_destinos -> vamos usar todos os destinos no RO
        diff_destino = soma_destinos  # quanto ainda falta "fechar" do destino

        for pos, row in df_o.iterrows():
            val_ori = float(row["VALOR"])

            if val_ori < diff_destino:
                df_RO = pd.concat([df_RO, df_o.iloc[[pos]]], ignore_index=True)
                diff_destino -= val_ori

            elif val_ori > diff_destino:
                # divide a linha de origem: parte fecha o destino (RO) e sobra vai para CA
                parte_RO = df_o.iloc[[pos]].copy()
                parte_RO["VALOR"] = diff_destino
                df_RO = pd.concat([df_RO, parte_RO], ignore_index=True)

                sobra = df_o.iloc[[pos]].copy()
                sobra["VALOR"] = val_ori - diff_destino
                df_CA = pd.concat([df_CA, sobra], ignore_index=True)

                if pos + 1 < len(df_o):
                    df_CA = pd.concat([df_CA, df_o.iloc[pos + 1 :]], ignore_index=True)
                break
            else:
                # val_ori == diff_destino
                df_RO = pd.concat([df_RO, df_o.iloc[[pos]]], ignore_index=True)

                if pos + 1 < len(df_o):
                    df_CA = pd.concat([df_CA, df_o.iloc[pos + 1 :]], ignore_index=True)
                break

        # depois de “fechar” o destino, todos os destinos vão pro RO
        df_RO = pd.concat([df_RO, df_d], ignore_index=True)

df_CA = pd.concat([df_CA, df_only_o, df_only_d], ignore_index=True)
# pronto: df_RO e df_CA com TODAS as CODACAO processadas

df_RO.to_csv(
    "df_RO.csv",
    index=False,
    sep=";",  # use ";" e "," se abrirá no Excel PT-BR
    decimal=",",
    encoding="utf-8-sig",
    # quoting=csv.QUOTE_MINIMAL,  # descomente se quiser controlar aspas
)
df_CA.to_csv(
    "df_CA.csv",
    index=False,
    sep=";",  # use ";" e "," se abrirá no Excel PT-BR
    decimal=",",
    encoding="utf-8-sig",
    # quoting=csv.QUOTE_MINIMAL,  # descomente se quiser controlar aspas
)
