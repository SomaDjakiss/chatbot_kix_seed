import os
import requests
import pandas as pd

def prepare_dataframe():
    login_url = "https://stats.seeds.bf/api/auth/login?annee_id=2024&ecole_db_name=undefined"
    login_payload = {
        "email": "abdoul@gmail.com",         # Remplace par ton email
        "password": "d96IQQWvZoig$%"          # Remplace par ton mot de passe
    }
    login_headers = {
        "Content-Type": "application/json",
        "Accept": "application/json"
    }

    login_response = requests.post(login_url, json=login_payload, headers=login_headers)

    if login_response.status_code == 200:
        token = login_response.json().get("token")

        # Étape 2 : Appel API avec le token
        data_url = "https://stats.seeds.bf/api/get_ia_datas/2024?page=1"
        data_headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json"
        }

        data_response = requests.get(data_url, headers=data_headers)

        if data_response.status_code == 200:
            data_api = data_response.json()
            df = pd.DataFrame(data_api["results"])

            # Copies de travail
            data_copy = df.copy()
            data_optmoyen = df.copy()

            # Suppression des colonnes inutiles
            data_copy.drop(columns=["optbulletinmoyennes", "retard_absence"], axis=1, inplace=True)

            # Conversion de la colonne 'optbulletins' en dictionnaire
            def convertir_optbulletins(liste_bulletins):
                if isinstance(liste_bulletins, list):
                    try:
                        return {b['trimestre']: b for b in liste_bulletins if 'trimestre' in b}
                    except Exception:
                        return liste_bulletins
                return liste_bulletins

            data_copy["optbulletins"] = data_copy["optbulletins"].apply(convertir_optbulletins)

            # Aplatissement des colonnes dictionnaires
            colonnes_dict = [col for col in data_copy.columns if data_copy[col].apply(lambda x: isinstance(x, dict)).any()]
            for col in colonnes_dict:
                temp_df = pd.json_normalize(data_copy[col])
                temp_df.columns = [f"{col}_{c}" for c in temp_df.columns]
                data_copy = pd.concat([data_copy.drop(columns=[col]), temp_df], axis=1)

            # Traitement de 'optbulletinmoyennes'
            df_exploded = data_optmoyen.explode('optbulletinmoyennes')
            df_exploded = df_exploded[df_exploded['optbulletinmoyennes'].notna()]
            optbulletin_flat = pd.json_normalize(df_exploded['optbulletinmoyennes'])
            optbulletin_flat = optbulletin_flat.add_prefix('optbmoyen_')

            df_exploded = df_exploded.reset_index(drop=True)
            optbulletin_flat = optbulletin_flat.reset_index(drop=True)

            colonnes_renommees = {
                "optbmoyen_eleve_id": "eleve_id",
                "optbmoyen_nom_prof": "nom_prof",
                "optbmoyen_matiere_id": "matiere_id",
                "optbmoyen_code_matiere": "code_matiere",
                "optbmoyen_libelle_matiere": "libelle_matiere",
                "optbmoyen_moyenne_compo": "note_matiere"
            }

            df_performance = optbulletin_flat[list(colonnes_renommees.keys())].rename(columns=colonnes_renommees)

            data_copy = data_copy.loc[:, ~data_copy.columns.duplicated()]
            df_merged = pd.merge(df_performance, data_copy, on="eleve_id", how="inner")

            # Traitement de 'retard_absence'
            retardabsence = data_optmoyen.explode('retard_absence')
            retardabsence = retardabsence[retardabsence['retard_absence'].notna()]
            retardabsence_flat = pd.json_normalize(retardabsence['retard_absence'])
            retardabsence_flat = retardabsence_flat.add_prefix('retard_absence_')

            retardabsence = retardabsence.reset_index(drop=True)
            retardabsence_flat = retardabsence_flat.reset_index(drop=True)

            retardabsence = retardabsence_flat.rename(columns={"retard_absence_eleve_id": "eleve_id"})
            df_merged_final = pd.merge(df_merged, retardabsence, on="eleve_id", how="inner")
            colonnes_a_supprimer = [
            "matiere_id", "code_matiere","id", "scolarite_id", "serie", "salle_classe_id",
            "updated_by", "created_by", "created_at", "updated_at", "ecole_id", "ecole_updated_at",
            "ecole_annee_id", "eleve_updated_by", "eleve_created_by", "eleve_deleted_at",
            "eleve_created_at", "eleve_updated_at", "eleve_annee_id",
            "classe_updated_by", "classe_created_by", "classe_created_at", "classe_updated_at",
            "salle_classe_annee_id", "salle_classe_updated_by", "salle_classe_created_by",
            "salle_classe_created_at", "salle_classe_updated_at", "salle_classe_classe.id",
            "salle_classe_classe.libelle", "salle_classe_classe.code", "salle_classe_classe.category",
            "salle_classe_classe.updated_by", "salle_classe_classe.created_by",
            "salle_classe_classe.created_at", "salle_classe_classe.updated_at",
            "optbulletins_T1.salle_classe_id", "optbulletins_T1.salle_classe",
            "optbulletins_T1.eleve_id", "optbulletins_T1.matricule", "optbulletins_T1.date_naissance",
            "optbulletins_T1.lieu_naissance", "optbulletins_T1.conduite", "optbulletins_T1.moyenne_t2",
            "optbulletins_T1.updated_by", "optbulletins_T1.created_by", "optbulletins_T1.created_at",
            "optbulletins_T1.updated_at", "optbulletins_T2.salle_classe_id", "optbulletins_T2.salle_classe",
            "optbulletins_T2.eleve_id", "optbulletins_T2.nom_prenom", "optbulletins_T2.date_naissance",
            "optbulletins_T2.lieu_naissance", "optbulletins_T2.conduite", "optbulletins_T2.updated_by",
            "optbulletins_T2.created_by", "optbulletins_T2.created_at", "optbulletins_T2.updated_at",
            "retard_absence_id", "retard_absence_created_at", "retard_absence_updated_at",
            "retard_absence_deleted_at"
        ]

        # Suppression des colonnes si elles existent
            df_merged_final = df_merged_final.drop(columns=[col for col in colonnes_a_supprimer if col in df_merged_final.columns])

            df_merged_final = df_merged_final.rename(columns={
                    # Identifiants principaux
            "eleve_id": "id_eleve",
            "nom_prof": "nom_enseignant",
            "matiere_id": "id_matiere",
            "code_matiere": "code_matiere",
            "libelle_matiere": "nom_matiere",
            "moyenne_compo": "notes_matieres",
            "annee_id": "id_annee",
            "scolarite_id": "id_scolarite",
            "classe_id": "id_classe",
            "salle_classe_id": "id_salle_classe",
            
            # Statut académique
            "etat_scolarite": "statut_scolarite",
            "redoublant": "est_redoublant",
            "affecte": "est_affecte",
            "src_photo": "source_photo",
            
            # Antécédents éducatifs de l'élève
            "eleve_frequent_preced": "frequentation_precedente",
            "eleve_frere_soeur_frequent": "freres_soeurs_frequentant",
            "eleve_bourse_etude": "a_bourse_etude",
            "eleve_classe_redouble_cp1": "redoublement_cp1",
            "eleve_classe_redouble_cp2": "redoublement_cp2",
            "eleve_classe_redouble_ce1": "redoublement_ce1",
            "eleve_classe_redouble_ce2": "redoublement_ce2",
            "eleve_classe_redouble_cm1": "redoublement_cm1",
            "eleve_classe_redouble_cm2": "redoublement_cm2",
            
            # Accès technologique de l'élève
            "eleve_possede_tel": "possede_telephone",
            "eleve_suivi_off": "suivi_officiel",
            "eleve_suivi_domicile": "suivi_a_domicile",
            "eleve_suivi_centre": "suivi_au_centre",
            "eleve_suivi_groupe": "suivi_en_groupe",
            
            # Matériel éducatif
            "eleve_mat_didact_table": "possede_bureau",
            "eleve_mat_didact_livres": "possede_livres",
            "eleve_mat_didact_tableaux": "possede_tableaux",
            "eleve_mat_didact_tablette": "possede_tablette",
            "eleve_mat_didact_autres": "possede_autres_materiels",
            
            # Équipements du foyer
            "eleve_menage_tele": "menage_a_television",
            "eleve_menage_radio": "menage_a_radio",
            "eleve_menage_internet": "menage_a_internet",
            "eleve_menage_electricite": "menage_a_electricite",
            "eleve_menage_autre": "menage_a_autres_equipements",
            
            # Informations sur l'école
            "ecole_ecole_nom": "nom_ecole",
            "ecole_ecole_code": "code_ecole",
            "ecole_ecole_annee_ouverture": "annee_ouverture_ecole",
            "ecole_ecole_statut": "statut_ecole",
            "ecole_ecole_situation_admin": "situation_administrative_ecole",
            "ecole_ecole_ref_arrete_ouverture": "reference_arrete_ouverture",
            "ecole_ecole_type": "type_ecole",
            "ecole_ecole_conventionel": "ecole_conventionnelle",
            "ecole_ecole_mode_recrutement": "mode_recrutement_ecole",
            "ecole_ecole_milieu": "milieu_ecole",
            "ecole_ecole_region": "region_ecole",
            "ecole_ecole_province": "province_ecole",
            "ecole_ecole_commune": "commune_ecole",
            "ecole_ecole_ceb": "ceb_ecole",
            "ecole_ecole_secteur_village": "secteur_village_ecole",
            
            # Informations sur le directeur d'école
            "ecole_ecole_directeur_nom_prenom": "nom_complet_directeur",
            "ecole_ecole_directeur_sexe": "sexe_directeur",
            "ecole_ecole_directeur_matrricule": "matricule_directeur",
            "ecole_ecole_directeur_emploi": "poste_directeur",
            "ecole_ecole_directeur_charge": "responsabilites_directeur",
            
            # Coordonnées de l'école
            "ecole_ecole_email": "email_ecole",
            "ecole_ecole_phone": "telephone_ecole",
            "ecole_ecole_boite_postal": "boite_postale_ecole",
            "ecole_ecole_logo": "logo_ecole",
            
            # Informations académiques de l'école
            "ecole_ecole_cycle": "cycle_ecole",
            "ecole_ecole_type_enseignement": "type_enseignement_ecole",
            "ecole_ecole_db_name": "nom_base_donnees_ecole",
            
            # Localisation de l'école
            "ecole_longitude": "longitude_ecole",
            "ecole_latitude": "latitude_ecole",
            
            # Informations personnelles de l'élève
            "eleve_file_name": "nom_fichier_eleve",
            "eleve_matricule": "matricule_eleve",
            "eleve_nom": "nom_eleve",
            "eleve_prenom": "prenom_eleve",
            "optbulletins_T1.nom_prenom": "nom_complet_eleve",
            "eleve_date_naissance": "date_naissance_eleve",
            "eleve_lieu_naissance": "lieu_naissance_eleve",
            "eleve_n_extrait": "numero_extrait_eleve",
            "eleve_genre": "genre_eleve",
            "eleve_telephone": "telephone_eleve",
            
            # Informations familiales de l'élève
            "eleve_nom_prenom_pere": "nom_complet_pere",
            "eleve_profession_pere": "profession_pere",
            "eleve_nom_prenom_mere": "nom_complet_mere",
            "eleve_profession_mere": "profession_mere",
            "eleve_nom_prenom_tel_pers_pre_besoin": "contact_urgence",
            
            # Conditions de vie de l'élève
            "eleve_vie_parents": "vit_avec_parents",
            "eleve_vie_chrez_parents": "vit_au_domicile_parents",
            "eleve_vie_chrez_tuteur": "vit_avec_tuteur",
            "eleve_eleve_statut": "statut_eleve",
            "eleve_eleve_handicap": "eleve_a_handicap",
            
            # Informations sur le bien-être de l'élève
            "eleve_eleve_victime_violence": "victime_violence",
            "eleve_victime_violence_physique": "victime_violence_physique",
            "eleve_victime_stigmatisation": "victime_stigmatisation",
            "eleve_victime_violence_sexuelle": "victime_violence_sexuelle",
            "eleve_victime_violence_emotionnelle": "victime_violence_emotionnelle",
            "eleve_victime_autre": "victime_autre_violence",
            
            # Informations supplémentaires sur l'élève
            "eleve_eleve_nationalite": "nationalite_eleve",
            "eleve_niveau_instruction_pere": "niveau_education_pere",
            "eleve_niveau_instruction_mere": "niveau_education_mere",
            "eleve_statut_mat_pere": "statut_matrimonial_pere",
            "eleve_statut_mat_mere": "statut_matrimonial_mere",
            "eleve_eleve_dort_moustiquaire": "dort_sous_moustiquaire",
            "eleve_eleve_distance_domicile": "distance_domicile",
            "eleve_eleve_moyen_deplacement": "mode_transport",
            "eleve_domicile_eleve": "residence_eleve",
            "eleve_iue": "identifiant_unique_eleve",
            
            # Informations sur la classe
            "classe_libelle": "nom_classe",
            "classe_code": "code_classe",
            "classe_category": "categorie_classe",
            
            # Informations sur la salle de classe
            "salle_classe_nbr_table": "nombre_tables_salle_classe",
            "salle_classe_libelle": "nom_salle_classe",
            "salle_classe_code": "code_salle_classe",
            
            # Informations du bulletin du premier trimestre
            "optbulletins_T1.id": "id_bulletin_t1",
            "optbulletins_T1.annee_scolaire": "annee_scolaire_t1",
            "optbulletins_T1.trimestre": "trimestre_t1",
            "optbulletins_T1.totaux": "points_totaux_t1",
            "optbulletins_T1.conduite_label": "appreciation_conduite_t1",
            "optbulletins_T1.sanction": "sanction_disciplinaire_t1",
            "optbulletins_T1.appreciation": "appreciation_enseignant_t1",
            "optbulletins_T1.plus_fort_moyenne": "moyenne_la_plus_elevee_t1",
            "optbulletins_T1.plus_faible_moyenne": "moyenne_la_plus_basse_t1",
            "optbulletins_T1.moyenne_classe": "moyenne_classe_t1",
            "optbulletins_T1.effectif": "effectif_classe_t1",
            "optbulletins_T1.rang": "rang_t1",
            "optbulletins_T1.moyenne": "moyenne_t1",
            "optbulletins_T1.moyenne_t1": "moyenne_periode1_t1",
            "optbulletins_T1.moyenne_annuel": "moyenne_annuelle_t1",
            "optbulletins_T1.rang_annuel": "rang_annuel_t1",
            "optbulletins_T1.decision_conseil": "decision_conseil_t1",
            "optbulletins_T1.composed": "compose_t1_finalise",
            "optbulletins_T1.motif_non_compose": "raison_non_compose_t1",
            
            # Informations du bulletin du deuxième trimestre
            "optbulletins_T2.id": "id_bulletin_t2",
            "optbulletins_T2.annee_scolaire": "annee_scolaire_t2",
            "optbulletins_T2.trimestre": "trimestre_t2",
            "optbulletins_T2.totaux": "points_totaux_t2",
            "optbulletins_T2.conduite_label": "appreciation_conduite_t2",
            "optbulletins_T2.sanction": "sanction_disciplinaire_t2",
            "optbulletins_T2.appreciation": "appreciation_enseignant_t2",
            "optbulletins_T2.plus_fort_moyenne": "moyenne_la_plus_elevee_t2",
            "optbulletins_T2.plus_faible_moyenne": "moyenne_la_plus_basse_t2",
            "optbulletins_T2.moyenne_classe": "moyenne_classe_t2",
            "optbulletins_T2.effectif": "effectif_classe_t2",
            "optbulletins_T2.rang": "rang_t2",
            "optbulletins_T2.moyenne": "moyenne_t2",
            "optbulletins_T2.moyenne_t1": "moyenne_periode1_t2",
            "optbulletins_T2.moyenne_t2": "moyenne_periode2_t2",
            "optbulletins_T2.moyenne_annuel": "moyenne_annuelle_t2",
            "optbulletins_T2.rang_annuel": "rang_annuel_t2",
            "optbulletins_T2.decision_conseil": "decision_conseil_t2",
            "optbulletins_T2.composed": "compose_t2_finalise",
            "optbulletins_T2.motif_non_compose": "raison_non_compose_t2",
            
            # Informations de présence et retard
            "retard_absence_annee_id": "id_annee_presence",
            "retard_absence_type": "type_presence",
            "retard_absence_heure_debut": "heure_debut_absence",
            "retard_absence_heure_fin": "heure_fin_absence",
            "retard_absence_date_debut": "date_debut_absence",
            "retard_absence_date_fin": "date_fin_absence",
            "retard_absence_date_abandon": "date_abandon",
            "retard_absence_motif": "motif_absence",
            "retard_absence_matiere_id1": "matiere_absence_id1",
            "retard_absence_matiere_id2": "matiere_absence_id2",
            "retard_absence_matiere_id3": "matiere_absence_id3",
            "retard_absence_demi_jounee": "absence_demie_journee"
        })

            return df_merged_final

        else:
            print("Erreur lors de la récupération des données :", data_response.status_code)
            return pd.DataFrame()
    else:
        print("Échec de la connexion :", login_response.status_code)
        return pd.DataFrame()

# Expose le DataFrame via une fonction
def get_df_merge_final():
    return prepare_dataframe()

if __name__ == "__main__":
    df = get_df_merge_final()
   

    
