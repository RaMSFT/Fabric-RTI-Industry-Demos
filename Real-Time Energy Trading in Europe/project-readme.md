# Step 1: 📥 Clone a GitHub Repository

Clone the repository **Fabric-RTI-Industry-Demos** into your own GitHub Reposiroty

[Fabric-RTI-Industry-Demos](https://github.com/RaMSFT/Fabric-RTI-Industry-Demos)
---

[Cloning a repository - GitHub Docs](https://docs.github.com/en/repositories/creating-and-managing-repositories/cloning-a-repository)
---

# Step 2: 🔗 GitHub Integration in Microsoft Fabric

Follow these steps to connect your Fabric workspace to a GitHub repository:

---

## ✅ 1. Create a Workspace
- In **Microsoft Fabric**, create a new workspace or select an existing one.
---

## ✅ 2. Open Workspace Settings
- Click **Workspace settings** from the top menu.
---

## ✅ 3. Navigate to Git Integration
- In the left panel, select **Git integration** under **General**.
---

## ✅ 4. Choose Git Provider
- Click **GitHub** as your Git provider.
---

## ✅ 5. Add GitHub Account
- Click **Add account**.
- Fill in the following details:
  - **Display name**: A name for your connection.
  - **Personal Access Token**: [Generate from GitHub](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/creating-a-personal-access-token).
  - **Repository URL**: The GitHub repository you want to connect.
---

## ✅ 6. Connect the Account
- Click **Connect** to establish the link.
---

## ✅ 7. Select a Branch
- Choose an existing branch or create a new one.
---

## ✅ 8. Sync Workspace
- Click **Connect and sync** to complete the integration.
---

### 📚 Additional Resources:
- [Git Integration](https://learn.microsoft.com/en-us/fabric/governance/git-integration)
- [Creating a Personal Access Token](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/creating-a-personal-access-token)
---

✅ Your Fabric workspace is now connected to GitHub for version control and collaboration!

---
# Step 3: 📝 Update the Notebook with Eventhouse Connection Details

 **Action Required:**  
Open your Fabric notebook and update the connection details for **Eventhouse**:

- Navigate to the **connection cell** in your notebook.
```
evh_name = "enter Event Hub name Here"
evh_conn_string = "eneter Event Hub Connection String"
```
- Replace placeholder values with:
  1. Open the Event House
  2. Click on on Source and navigate to and click on **SAS Key Authentication**
  3. Copy the Event hub name value and paste into the notebook variable **evh_name**
  4. Copy the Connection string-primary key value (click on eye symbol first) and paste into the notebook variable **evh_conn_string**
- Save the notebook after updating.

💡 *Tip:* Ensure the connection string matches the configuration in your Fabric


# Step 4: ▶️ Run the Notebook

🚀 **Action Required:**  
After updating the Eventhouse connection details in your Fabric notebook:

- Click **Run All** or execute cells individually

# Step 5: ✅ Activate All in Eventhouse

⚡ **Action Required:**  
Once the notebook has successfully run:

- Navigate to **Eventhouse** in your Fabric workspace.
- Click **Activate All** to enable:
  - Real-time ingestion
  - Processing rules
  - Any configured triggers or alerts

  **Once events start generating, wait a few minutes and you’ll see the data flowing into Eventhouse and reflected on your dashboards.**

