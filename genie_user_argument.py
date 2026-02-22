import json
from databricks.sdk import WorkspaceClient

def get_workspace_client() -> WorkspaceClient:
    """Initializes and returns the Databricks WorkspaceClient."""
    w = WorkspaceClient()
    return w

def get_space_details(w: WorkspaceClient, space_id: str) -> tuple:
    """Fetches space details and parses the serialized_space string into a dictionary."""
    response = w.api_client.do(
        "GET", 
        f"/api/2.0/genie/spaces/{space_id}", 
        query={"include_serialized_space": "true"}
    )
    # Parse the nested string into a usable Python dictionary
    serialized_space_dict = json.loads(response['serialized_space'])
    
    return response, serialized_space_dict

def update_column_description(serialized_space: dict, table_target: str, column_target: str, new_desc: str) -> dict:
    """Finds the target table and column, and appends the new description."""
    tables_list = serialized_space.get("data_sources", {}).get("tables", [])
    found_column = False

    for table in tables_list:
        if table.get('identifier') == table_target:
            for column in table.get('column_configs', []):
                if column.get('column_name') == column_target:
                    
                    # Cleanly append or create the list in one line
                    column.setdefault('description', []).append(new_desc)
                    print(f"\nSuccess! Updated description for '{column_target}': {column['description']}")
                    found_column = True
                    
    if not found_column:
        print(f"\nWarning: Could not find table '{table_target}' or column '{column_target}'. No changes made.")
                    
    return serialized_space

def patch_genie_space(w: WorkspaceClient, space_id: str, original_response: dict, updated_serialized_space: dict):
    """Packages the updated dictionary back into a string and sends the PATCH request."""
    # Convert the modified dict back into a "Scalar String"
    original_response['serialized_space'] = json.dumps(updated_serialized_space)
    
    # Send the PATCH request wrapped in a try-except block for safety
    try:
        w.api_client.do(
            "PATCH", 
            f"/api/2.0/genie/spaces/{space_id}", 
            body=original_response
        )
        print("Space updated successfully in Databricks!")
    except Exception as e:
        print(f"\nError: Failed to patch Genie space. API returned: {e}")

def main():
    print("--- Databricks Genie Space Updater ---")
    
    # 1. Ask the user for inputs (using .strip() to remove accidental spaces)
    space_id = input("Enter the Space ID: ").strip()
    table_target = input("Enter the target Table Name (e.g., samples.bakehouse.sales_customers): ").strip()
    column_target = input("Enter the target Column Name (e.g., customerID): ").strip()
    description_to_add = input("Enter the description to append: ").strip()

    # Basic validation to ensure they didn't leave anything blank
    if not all([space_id, table_target, column_target, description_to_add]):
        print("Error: All fields are required. Exiting script.")
        return

    # 2. Execute flow
    print("\nConnecting to Databricks...")
    w = get_workspace_client()
    
    try:
        print(f"Fetching data for Space ID: {space_id}...")
        raw_response, serialized_space_dict = get_space_details(w, space_id)
    except Exception as e:
        print(f"\nError: Could not find or access Space ID '{space_id}'. Details: {e}")
        return # Stop the script if the space doesn't exist
    
    # Modify data
    updated_space_dict = update_column_description(
        serialized_space=serialized_space_dict, 
        table_target=table_target, 
        column_target=column_target, 
        new_desc=description_to_add
    )
    
    # Save data
    print("Applying changes...")
    patch_genie_space(w, space_id, raw_response, updated_space_dict)

# Run the script
if __name__ == "__main__":
    main()
	
##############################################################
import json
from databricks.sdk import WorkspaceClient

def get_workspace_client() -> WorkspaceClient:
    """Initializes and returns the Databricks WorkspaceClient."""
    w = WorkspaceClient()
    # Note: WorkspaceClient usually handles auth automatically, 
    # but here is your token retrieval if your specific environment requires it elsewhere.
    token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
    print(f"Token retrieved: {token[:10]}...") 
    return w

def get_first_space_id(w: WorkspaceClient) -> str:
    """Fetches the list of Genie spaces and returns the ID of the first one."""
    response = w.api_client.do("GET", "/api/2.0/genie/spaces")
    space_id = response['spaces'][0]['space_id']
    print(f"Target Space ID: {space_id}")
    return space_id

def get_space_details(w: WorkspaceClient, space_id: str) -> tuple:
    """Fetches space details and parses the serialized_space string into a dictionary."""
    response = w.api_client.do(
        "GET", 
        f"/api/2.0/genie/spaces/{space_id}", 
        query={"include_serialized_space": "true"}
    )
    # Parse the nested string into a usable Python dictionary
    serialized_space_dict = json.loads(response['serialized_space'])
    
    return response, serialized_space_dict

def update_column_description(serialized_space: dict, table_target: str, column_target: str, new_desc: str) -> dict:
    """Finds the target table and column, and appends the new description."""
    tables_list = serialized_space.get("data_sources", {}).get("tables", [])

    for table in tables_list:
        if table.get('identifier') == table_target:
            for column in table.get('column_configs', []):
                if column.get('column_name') == column_target:
                    
                    # cleanly append or create the list in one line
                    column.setdefault('description', []).append(new_desc)
                    print(f"Updated description for {column_target}: {column['description']}")
                    
    # Because dictionaries are modified in-place in Python, 
    # serialized_space is already fully updated. We just return it.
    return serialized_space

def patch_genie_space(w: WorkspaceClient, space_id: str, original_response: dict, updated_serialized_space: dict):
    """Packages the updated dictionary back into a string and sends the PATCH request."""
    # Convert the modified dict back into a "Scalar String"
    original_response['serialized_space'] = json.dumps(updated_serialized_space)
    
    # Send the PATCH request
    # Note: Passing the dictionary directly to 'body=' is correct; 
    # the api_client will handle converting the outer payload to JSON.
    patch_response = w.api_client.do(
        "PATCH", 
        f"/api/2.0/genie/spaces/{space_id}", 
        body=original_response
    )
    print("Space updated successfully!")
    return patch_response

def main():
    # 1. Setup target variables
    TABLE_TARGET = 'samples.bakehouse.sales_customers'
    COLUMN_TARGET = 'customerID'
    DESCRIPTION_TO_ADD = "Date of Review"

    # 2. Execute flow
    w = get_workspace_client()
    space_id = get_first_space_id(w)
    
    # Fetch data
    raw_response, serialized_space_dict = get_space_details(w, space_id)
    
    # Modify data
    updated_space_dict = update_column_description(
        serialized_space=serialized_space_dict, 
        table_target=TABLE_TARGET, 
        column_target=COLUMN_TARGET, 
        new_desc=DESCRIPTION_TO_ADD
    )
    
    # Save data
    patch_genie_space(w, space_id, raw_response, updated_space_dict)

# Run the script
if __name__ == "__main__":
    main()
