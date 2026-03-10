#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Gerar Derivadas (simples, com “mover blocos com origem vazia para o final”)

Regras:
  1) D consecutivos (mesmo NUMERO, mesmo grupo) são somados.
  2) Para cada NUMERO de D, agregamos todas as suas ocorrências dentro do mesmo grupo,
     juntando as origens imediatamente anteriores de cada ocorrência e somando VALOR.
  3) Se QUALQUER D do bloco tiver SUBACAO preenchida => descarta o bloco (não emite).
  4) Se QUALQUER ORIGEM do bloco tiver SUBACAO vazia => o bloco é emitido,
     mas vai para o FINAL do CSV (origens do bloco + D somado).
"""

import argparse
import pandas as pd

# ---------- helpers básicos ----------

def norm_numero(v):
    s = str(v).strip()
    if s.endswith(".0"):
        s = s[:-2]
    return s

def is_dest(row, od_col):
    return str(row.get(od_col, "")).upper() == "D"

def group_key(row, group_cols):
    if not group_cols:
        return ()
    return tuple(row.get(c) for c in group_cols)

def is_filled(v):
    if v is None:
        return False
    s = str(v).strip()
    return s != "" and s.lower() not in ("nan", "none")

# ---------- Passo 1: colapsa D consecutivos ----------

def passo1(rows, od_col, num_col, val_col, group_cols):
    out = []
    i = 0
    n = len(rows)

    while i < n:
        r = rows[i]
        if not is_dest(r, od_col):
            out.append(r); i += 1; continue

        base = r.copy()
        total = int(base.get(val_col, 0))
        k_num = norm_numero(base.get(num_col))
        k_grp = group_key(base, group_cols)
        i += 1

        while i < n and is_dest(rows[i], od_col):
            if group_key(rows[i], group_cols) != k_grp:
                break
            if norm_numero(rows[i].get(num_col)) != k_num:
                break
            total += int(rows[i].get(val_col, 0))
            i += 1

        base[val_col] = total
        out.append(base)

    return out

# ---------- Passo 2/3/4: por segmento de grupo ----------

def processa_segmento_por_grupo(segment_rows, od_col, num_col, val_col, sub_col, debug=False):
    """
    segment_rows: trecho contínuo do MESMO grupo (ex.: AUTOR).

    1) Construímos "ocorrências" de D. Para cada D guardamos:
       - d_row, num (normalizado), valor, sub_ok (D tem SUBACAO?), origens (lista de O até esse D),
         has_origem_vazia (True se ALGUMA O dessa lista tem SUBACAO vazia).
    2) Agrupamos por NUMERO (na ordem do primeiro D):
       - Somamos valores de TODAS as ocorrências do NUMERO.
       - Unimos TODAS as origens dessas ocorrências (na ordem).
       - Se QUALQUER ocorrência tiver sub_ok=True -> descarta bloco (Passo 3).
       - Se QUALQUER ocorrência tiver has_origem_vazia=True -> bloco vai para o FINAL (Passo 4),
         senão o bloco fica no lugar.
    3) As O que restarem sem D após o último destino do segmento são copiadas ao final do segmento.
    """
    ocorrencias = []
    buffer_O = []

    # 1) escanear o segmento
    for r in segment_rows:
        if is_dest(r, od_col):
            num = norm_numero(r.get(num_col))
            val = int(r.get(val_col, 0))
            d_has_sub = is_filled(r.get(sub_col)) if (sub_col and sub_col in r) else False

            # origens associadas a este D = tudo que estava no buffer_O
            origens = list(buffer_O)
            buffer_O = []

            # origem vazia? (se não houver coluna, consideramos NENHUMA vazia)
            has_origem_vazia = False
            if sub_col:
                for o in origens:
                    if sub_col in o and not is_filled(o.get(sub_col)):
                        has_origem_vazia = True
                        break

            ocorrencias.append({
                "d_row": r.copy(),
                "num": num,
                "valor": val,
                "d_has_sub": d_has_sub,
                "origens": origens,
                "has_origem_vazia": has_origem_vazia,
            })
        else:
            buffer_O.append(r)

    # 2) agrupar por NUMERO
    inline_out = []   # blocos normais (ficam no lugar)
    tail_out = []     # blocos que vão para o final (Passo 4)
    usados = set()

    for i in range(len(ocorrencias)):
        if i in usados:
            continue
        alvo = ocorrencias[i]["num"]

        soma = 0
        todas_origens = []
        descartar_todo_bloco = False
        mover_para_final = False
        idxs = []

        for j in range(i, len(ocorrencias)):
            if j in usados:
                continue
            if ocorrencias[j]["num"] == alvo:
                idxs.append(j)
                soma += ocorrencias[j]["valor"]
                todas_origens.extend(ocorrencias[j]["origens"])
                descartar_todo_bloco = descartar_todo_bloco or ocorrencias[j]["d_has_sub"]
                mover_para_final = mover_para_final or ocorrencias[j]["has_origem_vazia"]

        for j in idxs:
            usados.add(j)

        if debug:
            print(f"[BLOCO num={alvo}] ocor={len(idxs)} O={len(todas_origens)} soma={soma} "
                  f"descartar={descartar_todo_bloco} mover_final={mover_para_final}")

        if descartar_todo_bloco:
            # Passo 3: não emite nada
            continue

        # monta o D somado (base = 1ª ocorrência)
        d = ocorrencias[i]["d_row"].copy()
        d[val_col] = soma

        bloco = todas_origens + [d]

        # Passo 4: se tem origem vazia no bloco -> manda para o final
        if mover_para_final:
            tail_out.extend(bloco)
        else:
            inline_out.extend(bloco)

    # 3) sobras de O (sem D depois) ficam no final do segmento “normal”
    inline_out.extend(buffer_O)

    return inline_out, tail_out

def passo2_3_4(rows, od_col, num_col, val_col, group_cols, sub_col, debug=False):
    """
    Divide a planilha em segmentos consecutivos do MESMO grupo (ex.: AUTOR).
    Em cada segmento, aplica processa_segmento_por_grupo e acumula:
      - parte “inline” (normal) na ordem,
      - parte “tail” (mover para o final) em um buffer para anexar no fim.
    """
    final_inline = []
    final_tail = []

    if not rows:
        return []

    start = 0
    n = len(rows)
    while start < n:
        g0 = group_key(rows[start], group_cols)
        end = start + 1
        while end < n and group_key(rows[end], group_cols) == g0:
            end += 1

        seg = rows[start:end]
        inline_out, tail_out = processa_segmento_por_grupo(
            seg, od_col, num_col, val_col, sub_col, debug=debug
        )
        final_inline.extend(inline_out)
        final_tail.extend(tail_out)

        start = end

    # anexa os blocos “movidos” ao final, mantendo ordem relativa entre eles
    return final_inline + final_tail

# ---------- MAIN ----------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="CSV de entrada (base bruta)")
    ap.add_argument("--output", required=True, help="CSV de saída (derivadas)")
    ap.add_argument("--sep", default=";", help="Separador CSV (padrão ;)")
    ap.add_argument("--encoding", default="utf-8", help="Encoding (utf-8 | latin1)")
    ap.add_argument("--group-cols", default="AUTOR",
                    help='Colunas de barreira separadas por vírgula. Ex.: "AUTOR" ou "AUTOR,UO" (padrão: AUTOR)')
    ap.add_argument("--subacao-col", default="SUBACAO", help='Nome da coluna SUBACAO (padrão: "SUBACAO")')
    ap.add_argument("--debug", action="store_true", help="Imprime blocos durante o processamento")
    args = ap.parse_args()

    df = pd.read_csv(args.input, sep=args.sep, engine="python", encoding=args.encoding)

    # nomes reais das colunas (case-insensitive)
    lower = {c.lower(): c for c in df.columns}
    for req in ["od", "numero", "valor"]:
        if req not in lower:
            raise SystemExit(f"Faltou coluna '{req.upper()}'. Colunas: {list(df.columns)}")
    OD, NUM, VAL = lower["od"], lower["numero"], lower["valor"]

    # colunas de grupo (barreiras)
    group_cols = []
    if args.group_cols:
        for g in [s.strip() for s in args.group_cols.split(",") if s.strip()]:
            key = g.lower()
            if key not in lower:
                raise SystemExit(f"Coluna de barreira '{g}' não existe.")
            group_cols.append(lower[key])

    # coluna SUBACAO (pode não existir)
    sub_col = None
    if args.subacao_col and args.subacao_col.lower() in lower:
        sub_col = lower[args.subacao_col.lower()]

    # VALOR numérico
    df[VAL] = pd.to_numeric(df[VAL], errors="coerce").fillna(0).astype(int)

    rows = df.to_dict(orient="records")

    # Passo 1
    step1 = passo1(rows, OD, NUM, VAL, group_cols)

    # Passo 2 + 3 + 4 (agora com mover para o final quando tiver origem vazia)
    step2 = passo2_3_4(step1, OD, NUM, VAL, group_cols, sub_col, debug=args.debug)

    out_df = pd.DataFrame(step2, columns=df.columns)
    out_df.to_csv(args.output, sep=args.sep, index=False, encoding=args.encoding)
    print(f"OK -> {args.output} | linhas: {len(out_df)}")

if __name__ == "__main__":
    main()
