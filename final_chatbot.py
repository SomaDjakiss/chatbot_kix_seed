import os
import re
import streamlit as st
from collections import defaultdict
from itertools import islice
from langchain.chains import ConversationalRetrievalChain
from langchain.chains.question_answering import load_qa_chain
from langchain.prompts import PromptTemplate
from langchain.memory import ConversationBufferMemory
from dotenv import load_dotenv
from langchain.chat_models import ChatOpenAI
from RGA_TEST_SQL import get_df_merge_final

# chargement de donnees
df_merge_final = get_df_merge_final()

# Charger les variables d'environnement
load_dotenv()

# Utiliser la clé API depuis .env
openai_api_key = os.getenv("OPENAI_API_KEY")

llm = ChatOpenAI(
    model_name="gpt-4",
    temperature=0.7,
    openai_api_key=openai_api_key
)

# Initialisation du modèle
#llm = ChatOllama(
 #   model="llama3.2",
 #   temperature=0.7,
#)


# Prompt intelligent
prompt_template = PromptTemplate(
    input_variables=["question", "donnees"],
    template = """
Tu es un expert en analyse pédagogique.

Voici une série de données sur les performances scolaires d'élèves d'une même classe. Chaque bloc correspond à un élève, avec ses informations et ses notes de chaque  trimestre.

Ta tâche est de faire une analyse globale de la classe, en :
identifiant le niveau général des élèves,
repérant les matières les mieux et moins bien réussies,
indiquant s'il existe des cas exceptionnels (très bons ou très faibles),
proposant des recommandations pour améliorer les performances globales.

Question : {question}

Voici les données disponibles sur l'élève :
{donnees}

Rédige une réponse structurée, claire et concise.
"""
)

def get_response_from_dataframe(question, df_merge_final, nb_eleves=3):
    reponses = []

    # Vérifier si la question contient un identifiant d’élève
    identifiants = [str(id_) for id_ in df_merge_final['id_eleve'].unique() if str(id_) in question]

    if identifiants:
        for identifiant in identifiants:
            ligne = df_merge_final[df_merge_final['id_eleve'] == identifiant].iloc[0]
            donnees_texte = "\n".join([f"{col} : {ligne[col]}" for col in df_merge_final.columns])
            prompt = prompt_template.format(question=question, donnees=donnees_texte)
            resultat = llm.invoke(prompt)
            if hasattr(resultat, 'content'):
                resultat = resultat.content
            reponses.append(str(resultat))
        return "\n\n---\n\n".join(reponses)

    # Analyse collective : chercher le nom d’école et le code de classe dans la question
    ecoles_possibles = df_merge_final['nom_ecole'].unique()
    classes_possibles = df_merge_final['code_classe'].unique()

    nom_ecole = next((ecole for ecole in ecoles_possibles if ecole.lower() in question.lower()), None)
    code_classe = next((classe for classe in classes_possibles if classe.lower() in question.lower()), None)

    if nom_ecole and code_classe:
        df_filtre = df_merge_final[
            (df_merge_final['nom_ecole'].str.lower() == nom_ecole.lower()) &
            (df_merge_final['code_classe'].str.lower() == code_classe.lower())
        ]

        if df_filtre.empty:
            return f"Aucun élève trouvé pour l'école '{nom_ecole}' et la classe '{code_classe}'."

        # Limiter le nombre d’élèves analysés
        df_limite = df_filtre.head(nb_eleves)

        # Analyse par groupes pour éviter les débordements de tokens
        colonnes_utiles = ['id_eleve', 'notes_matieres'
                           'statut_scolarite', 'moyenne_t1']

        for i in range(0, len(df_limite), 3):  # groupe de 10 max
            chunk = df_limite.iloc[i:i + 3]
            donnees_texte = "\n\n".join([
                "\n".join([f"{col} : {ligne[col]}" for col in colonnes_utiles if col in ligne])
                for _, ligne in chunk.iterrows()
            ])
            prompt = prompt_template.format(question=question, donnees=donnees_texte)
            resultat = llm.invoke(prompt)
            if hasattr(resultat, 'content'):
                resultat = resultat.content
            reponses.append(str(resultat))

        return "\n\n---\n\n".join(reponses)

    return "Je n'ai pas trouvé d'identifiant d' élève, ni d' école et classe dans votre question."


# Configuration Streamlit
st.set_page_config(page_title="Chat GPT 4", layout="centered")
st.title("🎓 Chatbot Scolaire - Performances Élève")

if "chat_history" not in st.session_state:
    st.session_state.chat_history = []

# Zone de saisie utilisateur
user_input = st.chat_input("Pose une question ")

if user_input:
    response = get_response_from_dataframe(user_input, df_merge_final)

    # On stocke uniquement les messages (sans les afficher directement)
    st.session_state.chat_history.extend([
        {"role": "user", "content": user_input},
        {"role": "assistant", "content": response}
    ])

# Affichage unique basé sur l’historique
for message in st.session_state.chat_history:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

#print(df_merge_final.head())