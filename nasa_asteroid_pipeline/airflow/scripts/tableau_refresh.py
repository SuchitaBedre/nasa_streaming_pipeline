import os
import tableauserverclient as TSC

def trigger_tableau_refresh(
    server_url="https://prod-useast-a.online.tableau.com",
    site_id="YOUR_TABLEAU_SITE_ID",
    token_name="YOUR_PAT_NAME",
    token_value="YOUR_PAT_SECRET",
    datasource_name="gold_asteroids_tableau"
):
    """
    Triggers a direct extract refresh on Tableau Cloud/Server once Gold processing finishes.
    """
    print(f"Connecting to Tableau Server at {server_url}...")
    tableau_auth = TSC.PersonalAccessTokenAuth(token_name, token_value, site_id=site_id)
    server = TSC.Server(server_url, use_server_version=True)

    try:
        with server.auth.sign_in(tableau_auth):
            print("Authenticated successfully with Tableau Server/Cloud.")
            
            # Fetch target datasource
            datasources, _ = server.datasources.get()
            target_ds = next((ds for ds in datasources if ds.name == datasource_name), None)
            
            if target_ds:
                job = server.datasources.refresh(target_ds.id)
                print(f"🚀 Triggered Tableau Refresh Job ID: {job.id}")
                return True
            else:
                print(f"⚠️ Warning: Datasource '{datasource_name}' not found on Tableau Cloud.")
                return False
    except Exception as e:
        print(f"❌ Failed to trigger Tableau Refresh: {str(e)}")
        return False