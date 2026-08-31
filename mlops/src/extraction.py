#!pip install pdfplumber
import pdfplumber
import re
from collections import namedtuple
import sys
pdf_path = sys.argv[1]

with pdfplumber.open(pdf_path) as pdf:
    page=pdf.pages[0]
    text=page.extract_text()
for line in text.split("\n"):
    print(line)

date_re = re.compile(r"(?<=Date:\s)\d{2}/\d{2}/\d{4}")
date = date_re.search(text)
print(date)

print(date.group())
date = date.group()
numero_Facture_re = re.compile(r"(?<=Facture\s#:\s).+")
numero_Facture = numero_Facture_re.search(text)
numero_Facture = numero_Facture.group()
print(numero_Facture)

TVA_re = re.compile(r"(?<=TVA\s).+")
TVA = TVA_re.search(text)
TVA = TVA.group()
print(TVA)

id_Client_re = re.compile(r"(?<=Client ID\s).+")
id_Client = id_Client_re.search(text)
id_Client = id_Client.group()
print(id_Client)
nom_client_re = re.compile(r"(?<=Nom SOUGUI E SHOP Nom).+")
nom_client = nom_client_re.search(text)
nom_client =nom_client.group()
print(nom_client)

Matricule_client_re = re.compile(r"(?<=Matricule Fiscal ).+")
Matricule_client = Matricule_client_re.search(text)
Matricule_client = Matricule_client.group()
print(Matricule_client)

import re

lines = text.split("\n")
result = ""

for i, line in enumerate(lines):
    if re.search(r"\bAdresse\s+Adresse\b", line):
        before = lines[i-1] if i > 0 else ""
        current = line
        after = lines[i+1] if i < len(lines) - 1 else ""

        combined = f"{before} {current} {after}"

        # phrases à supprimer (espaces variables)
        patterns = [
            r"18\s*Rue\s*Mokhtar\s*Attia\s*Megrine\s*,?\s*Ben",
            r"Arous\s*2033",
            r"Adresse\s+Adresse"
        ]

        for p in patterns:
            combined = re.sub(p, "", combined, flags=re.IGNORECASE)

        # nettoyage final
        result = re.sub(r"\s{2,}", " ", combined).strip()
        break
adresse_client = result
print(adresse_client)
import pymupdf
doc = pymupdf.open(pdf_path)
page = doc[0]
tables = page.find_tables()
tables.tables[0].to_pandas()
page.search_for("Description ")
page.search_for("Téléphone ")
page.search_for("Conditions de paiement")

conditions_y0 = page.search_for("Conditions de paiement")[0][1]
page = doc[0]
tables = page.find_tables(clip=(0,230,page.rect[2], 550))


tables.tables[0].to_pandas()
page = doc[0]
tables = page.find_tables(horizontal_strategy = "text",clip=(0,230,page.rect[2],page.rect[3]))


tables.tables[0].to_pandas()
page.search_for("Montant Total")
page.search_for("Référence")
reference_x0 = page.search_for("Référence")[0][0]
print(reference_x0)
reference_x1 = page.search_for("Référence")[0][2]
print(reference_x1)
page.search_for("Description")

Description_x0 = page.search_for("Description")[0][0]
print(Description_x0)
Description_x1 = page.search_for("Description")[0][2]
print(Description_x1)
page.search_for("PU HT")

PU_HT_x0 = page.search_for("PU HT")[0][0]
print(PU_HT_x0)
PU_HT_x1 = page.search_for("PU HT")[0][2]
print(PU_HT_x1)
page.search_for("Quantité")

Quantité_x0 = page.search_for("Quantité")[0][0]
print(Quantité_x0)
Quantité_x1 = page.search_for("Quantité")[0][2]
print(Quantité_x1)
TVA_x0 = page.search_for("TVA")[0][0]
print(Quantité_x0)
TVA_x1 = page.search_for("TVA")[0][2]
print(TVA_x1)
page.search_for("Prix	Total HT")
page.search_for("TND")
DNT_Montant = page.search_for("TND")[3]
DNT_Montant_x0 = page.search_for("TND")[0][0]
print(DNT_Montant_x0)
DNT_Montant_x1 = page.search_for("TND")[0][2]
print(DNT_Montant_x1)
Prix_Total_HT_x0 = page.search_for("Prix	Total HT")[0][0]
print(Quantité_x0)
Prix_Total_HT_x1 = page.search_for("Prix	Total HT")[0][2]
print(Prix_Total_HT_x1)
#xligne2 =(Description_x0 + reference_x1)/2
xligne2 = reference_x1 +5
#xlignez = Description_x0 -5
#xligne3 =(Description_x1 + PU_HT_x0)/2
xligne3 = PU_HT_x0 -10
#xligne4 = (PU_HT_x1 + Quantité_x0)/2
xligne4 = Quantité_x0
#xligne5 = (Quantité_x1 + Prix_Total_HT_x0 )/2
xligne5 = Quantité_x1 +10

xligne6 = Quantité_x1 +1000
xligne7 = (TVA_x0 + Description_x1)/2
print (xligne2 , xligne3 , xligne4 , xligne5)
print(page.rect[0], page.rect[2])
vertical_lines = [page.rect[0] ,xligne2 , xligne3 , xligne4 ,xligne5,xligne6,xligne7 , page.rect[2]-5000]
tables = page.find_tables(vertical_lines = vertical_lines ,clip=(0,230,page.rect[2] , conditions_y0 -10) , snap_y_tolerance=7.8)
df =tables[0].to_pandas()
df["Description"]= df["Col2"]
df = df.drop(columns=["Col2","Col3","Col6"])
df["Numéro Facture"] = numero_Facture
df["id_Client"] = id_Client
df["Date"] = date
df["Client Nom"] = nom_client
df["Matricule Fiscal"] = Matricule_client
df["Adresse"] = adresse_client
df["TVA"] = TVA
df = df[
    [
        "Référence",
        "Description",
        "PU HT",
        "Quantité",
        "Prix Total HT",
        "Numéro Facture",
        "id_Client",
        "Date",
        "Client Nom",
        "Matricule Fiscal",
        "Adresse",
        "TVA",
    ]
]
df = df[
    df["Référence"].notna() &
    (df["Référence"].str.strip() != "")
]
cols = ["Quantité", "PU HT", "Prix Total HT"]
for c in cols:
    df[c] = df[c].fillna("").astype(str).str.strip()

df = df[~(
    (df["Quantité"] == "") &
    (df["PU HT"] == "") &
    (df["Prix Total HT"] ==  "")
)]
df = df.reset_index(drop=True)
output_path = sys.argv[2] if len(sys.argv) > 2 else "output.xlsx"
df.to_excel(output_path, index=False)