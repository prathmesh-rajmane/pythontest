# -*- coding: utf-8 -*-
"""
Last Updated on 10 Aug 2023

@author: trajeev

Functionality:
    
This file does the following jobs:
    * Reads-in the 411x411 data and the model outputs file created by DetailLevel_Model.py
    * Reads-in the Year wise and Produced/Purchased adjustments
    * Dynamically gives the adjustment values for any combination of a Input Commodity and the year
     i.e. Year price adjusment factor for 1111A0/US for  year 2011 --> 1.116071
    * Reads in the "Detail_Dashboard_UserInput.xlsx" file from the INPUTS folder.
    * Performs the dashboard Calculations and saves the S1, S2, S3 results for the company as a whole.
    * Also saves the Scope 3 breakdown for the company (Top 100 Commodities)
    * Scope 3 breakdown are also saved in Summary and Consolidated formats.
    * The Outputs are saved to "Detail_Dashboard_Outputs.xlsx" of the OUTPUTS folder.
"""
import pandas as pd
import json
import numpy as np
import warnings
import boto3
warnings.filterwarnings('ignore')

#***************** SECTION 1: VARIABLE INITIALIZATION *****************
#######################################
# Initializing all variables required
#######################################

Full_data_411_411_df, Model_OP_411_df, code_mapping_df = pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
dashboard_userinput_df, Year_Adjustment_df, Prod_Purch_adj_df, calculations_df, Tier_Counts_df, Tier_Percentages_df, Tier_Commodities_df = pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
Scope3_Breakdown_df, Scope3_Breakdown_df_DR_Sorted, Analysis_Tableau_df, Analysis_2_df, Final_results_df, companies_seg_df, Tier_Chart_df, Complete_Tier_Chart_df = pd.DataFrame(),pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
Scope3_Breakdown_Summary_df, Scope3_Breakdown_Consolidated_df, Scope3_Breakdown_Summary_df_DR_Sorted, Scope3_Breakdown_Consolidated_df_DR_Sorted = pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
Tier_Commodity_Calculations_df, Tier1_S3_df, Tier2_S1_df, Tier2_S2_df, Tier2_S3_df, Tier3_S1_df, Tier3_S2_df, Tier3_S3_df, Tier4_S1_df, Tier4_S2_df, Tier4_S3_df = pd.DataFrame(),pd.DataFrame(),pd.DataFrame(),pd.DataFrame(),pd.DataFrame(),pd.DataFrame(),pd.DataFrame(),pd.DataFrame(),pd.DataFrame(),pd.DataFrame(),pd.DataFrame()
commodities_list = []
Purchased_Commodities_list = []
Total_Company_Carbon_emissions, Total_Company_Scope1 , Total_Company_Scope2, Total_Company_Scope3 = 0,0,0,0
Percentage_Company_Scope1,Percentage_Company_Scope2, Percentage_Company_Scope3=0,0,0
Year = 0
# excel_path = r"/home/ymakadia/Downloads/DB_MODEL_BACKEND_AUGUST14/INPUTS/"

#***************** SECTION 2: MODEL OUTPUTS AND INPUT FILES READ IN *****************
#######################################
# FileReadin
# Purpose           : To read in the User input dashboard excel file, 411x411 data,  411 emissions Model output,
#                     Get Year wise adjustment value,  Get Produced/Purchased adjustment value
#######################################


def Read_Static_Excel_Files():

    global Full_data_411_411_df, Model_OP_411_df, Model_OP_92_df, Model_OP_22_df, Tier_Counts_df, Tier_Percentages_df, Tier_Commodities_df, code_mapping_df, Year_Adjustment_df, Prod_Purch_adj_df

    try:

        s3 = boto3.client('s3')
        bucket_name = 'demandbetter-aws-dev'
        
        s3_object = s3.get_object(Bucket=bucket_name, Key='Full_data_411_411.xlsx')
        Full_data_411_411_df = pd.read_excel(s3_object['Body'], sheet_name="411x411")
        # Full_data_411_411_df = pd.read_excel(excel_path + r"Full_data_411_411.xlsx", sheet_name="411x411")
        Full_data_411_411_df.set_index("OP_Comm",inplace=True)
        print("411x411 data is read!")

        s3_object = s3.get_object(Bucket=bucket_name, Key='Detail_Model_Emissions.xlsx')
        Model_OP_411_df = pd.read_excel(s3_object['Body'], sheet_name="Detail_Model_OPs")
        # Model_OP_411_df = pd.read_excel(excel_path + r"Detail_Model_Emissions.xlsx", sheet_name="Detail_Model_OPs")
        Model_OP_411_df.set_index("Commodity_Code",inplace=True)
        print("Model Outputs - 411 data is read!")

        s3_object = s3.get_object(Bucket=bucket_name, Key='Summary_Consolidated_Emissions.xlsx')
        Model_OP_92_df = pd.read_excel(s3_object['Body'], sheet_name="92 - Emissions")
        # Model_OP_92_df = pd.read_excel(excel_path + r"Summary_Consolidated_Emissions.xlsx", sheet_name="92 - Emissions")
        Model_OP_92_df.set_index("Description",inplace=True)
        print("Model Outputs - 92 data is read!")

        s3_object = s3.get_object(Bucket=bucket_name, Key='Summary_Consolidated_Emissions.xlsx')
        Model_OP_22_df = pd.read_excel(s3_object['Body'], sheet_name="22 - Emissions")
        # Model_OP_22_df = pd.read_excel(excel_path + r"Summary_Consolidated_Emissions.xlsx", sheet_name="22 - Emissions")
        Model_OP_22_df.set_index("Description",inplace=True)
        print("Model Outputs - 22 data is read!")

        print("Reading in Tier results....")

        s3_object = s3.get_object(Bucket=bucket_name, Key='Tier_Results.xlsx')
        Tier_Counts_df = pd.read_excel(s3_object['Body'], sheet_name="Tier_Counts")
        # Tier_Counts_df = pd.read_excel(excel_path + r"Tier_Results.xlsx", sheet_name="Tier_Counts")
        Tier_Counts_df.set_index("Output_Commodity",inplace=True)
        print("Tier Counts data is read!")

        s3_object = s3.get_object(Bucket=bucket_name, Key='Tier_Results.xlsx')
        Tier_Percentages_df = pd.read_excel(s3_object['Body'], sheet_name="Tier_Percentages")
        # Tier_Percentages_df = pd.read_excel(excel_path + r"Tier_Results.xlsx", sheet_name="Tier_Percentages")
        Tier_Percentages_df.set_index("Output_Commodity",inplace=True)
        print("Tier Percentage data is read!")

        s3_object = s3.get_object(Bucket=bucket_name, Key='Tier_Results.xlsx')
        Tier_Percentages_df = pd.read_excel(s3_object['Body'], sheet_name="Tier_Commodities")
        # Tier_Commodities_df = pd.read_excel(excel_path + r"Tier_Results.xlsx", sheet_name="Tier_Commodities")
        Tier_Commodities_df.set_index("Output_Commodity",inplace=True)
        print("Tier Commodities data is read!")

        s3_object = s3.get_object(Bucket=bucket_name, Key='Code_Mapping.xlsx')
        code_mapping_df = pd.read_excel(s3_object['Body'], sheet_name="Code_Mapping")
        # code_mapping_df = pd.read_excel(excel_path + r"Code_Mapping.xlsx", sheet_name="Code_Mapping")
        code_mapping_df.set_index("Detail Commodity Code - 411 level",inplace=True)
        code_mapping_df["Summary Level Commodity Code - 92 level"] = code_mapping_df["Summary Level Commodity Code - 92 level"].astype(str)
        code_mapping_df["Consolidated Level Commodity Code - 22 level"] = code_mapping_df["Consolidated Level Commodity Code - 22 level"].astype(str)
        print("Code Mapping data is read")

        s3_object = s3.get_object(Bucket=bucket_name, Key='Year_Adjust.xlsx')
        temp_year_adj = pd.read_excel(s3_object['Body'], sheet_name="Year Price Adjust")
        # temp_year_adj = pd.read_excel(excel_path + r"Year_Adjust.xlsx", sheet_name="Year Price Adjust")
        temp_year_adj.drop(["Summary Commodity", "Consolidated Commodity"],axis=1,inplace=True)
        temp_year_adj.set_index("Detail Commodity",inplace=True)
        Year_Adjustment_df = temp_year_adj.copy(deep=True)
        print("Year Adjust sheet Year Price Adjust is read")
        
        # temp_prod_purch_adj = pd.read_excel(excel_path + r"Year_Adjust.xlsx", sheet_name="Producer-Purchaser Price Adjust")
        s3_object = s3.get_object(Bucket=bucket_name, Key='Year_Adjust.xlsx')
        temp_prod_purch_adj = pd.read_excel(s3_object['Body'], sheet_name="Producer-Purchaser Price Adjust")
        temp_prod_purch_adj.drop(["Summary Commodity", "Consolidated Commodity"],axis=1,inplace=True)
        temp_prod_purch_adj.set_index("Detail Commodity",inplace=True)
        Prod_Purch_adj_df = temp_prod_purch_adj.copy(deep=True)
        print("Year Adjust sheet Producer-Purchaser Price Adjust is read")

        print("Static files reading done......")

    except Exception as e:
        return e
    
    return

    
## To get the Total Emission Factor Value for the OP Commodity Code
def Get_ModelOP_Value(OP_Comm_Code):
    OP_extract = {
                   "OP_Comm_Code":OP_Comm_Code,
                   "Scope_1": float(Model_OP_411_df["Percent_Scope1"][OP_Comm_Code]),
                   "Scope_2": float(Model_OP_411_df["Percent_Scope2"][OP_Comm_Code]),
                   "Scope_3": float(Model_OP_411_df["Percent_Scope3"][OP_Comm_Code]),
                   "TEF_Val":float(Model_OP_411_df["Total"][OP_Comm_Code]) 
                   }
    OP_JSON = json.dumps(OP_extract)
    return OP_JSON 

## Get Year based adjustment factor for a commodity
def Get_Year_adj_Value(OP_Comm_Code,Year):
    Yr_adj_extract = {"Year":Year, "OP_Comm_Code":OP_Comm_Code, "Year_Adj":float(Year_Adjustment_df[Year][OP_Comm_Code]) }
    Yr_adj_JSON = json.dumps(Yr_adj_extract)
    return Yr_adj_JSON   

## Get Producer/Purchased based adjustment factor for a commodity
def Get_Prod_Purchaser_Value(OP_Comm_Code,Year):
    Pr_Pur_adj_extract = {"Year":Year, "OP_Comm_Code":OP_Comm_Code, "Pro_Pur_adj":float(Prod_Purch_adj_df[Year][OP_Comm_Code]) }
    Pr_Pur_adj_JSON = json.dumps(Pr_Pur_adj_extract)
    return Pr_Pur_adj_JSON   

## Get the revenue entered by the user for each commodity
def Get_Revenue_Summary_Value(OP_Comm_Code):
    global dashboard_userinput_df
    temp_pivot_rev_table = pd.pivot_table(dashboard_userinput_df,index=["Commodity Code"], aggfunc = np.sum)
    Revenue_extract = {"OP_Comm_Code": OP_Comm_Code, "Revenue": float(temp_pivot_rev_table["Revenue ($)"][OP_Comm_Code])}
    Revenue_extract_JSON = json.dumps(Revenue_extract)
    return Revenue_extract_JSON

## Read in Code-Mapping file - Detail level
def Get_CodeMapping_Value(Comm_Code):
    global code_mapping_df
    temp_extract = code_mapping_df.loc[Comm_Code,"Detail Commodity Code description"]
    summary_code_extract = code_mapping_df.loc[Comm_Code,"Summary Level Commodity Code - 92 level"]
    consolidated_code_extract = code_mapping_df.loc[Comm_Code,"Consolidated Level Commodity Code - 22 level"]
    Summary_Comm_Code_desc = code_mapping_df.loc[Comm_Code,"Summary Commodity Code description"]
    Consolidated_Comm_Code_desc = code_mapping_df.loc[Comm_Code,"Consolidated Commodity Code description"]
    #print (temp_extract,summary_code_extract,consolidated_code_extract)
    comm_code_extract = {"Detail_Comm_Code": str(Comm_Code), "Detail_Comm_Code_desc": temp_extract,
                         "Summary_Comm_Code": str(summary_code_extract), "Consolidated_Comm_Code": str(consolidated_code_extract),
                         "Summary_Comm_Code_desc": Summary_Comm_Code_desc, "Consolidated_Comm_Code_desc": Consolidated_Comm_Code_desc } 
    
    comm_code_JSON = json.dumps(comm_code_extract)
    return comm_code_JSON


## Read in Code-Mapping file - Summary level
def Get_CodeMapping_Value_Summary(Summ_Comm_Code):
    global code_mapping_df   
    summary_code_mapping_df = code_mapping_df.copy(deep=True)
    summary_code_mapping_df.reset_index()
    summary_code_mapping_df.set_index("Summary Level Commodity Code - 92 level", inplace=True)
    temp_extract = summary_code_mapping_df.loc[Summ_Comm_Code,"Summary Commodity Code description"][0]
    if len(temp_extract) == 1:
        temp_extract = summary_code_mapping_df.loc[Summ_Comm_Code,"Summary Commodity Code description"]
    comm_code_extract = {"Summ_Comm_Code": str(Summ_Comm_Code), "Summary_Comm_Code_desc": temp_extract} 
    comm_code_JSON = json.dumps(comm_code_extract)
    return comm_code_JSON


## Read in Code-Mapping file - Consolidated level
def Get_CodeMapping_Value_Cons(Cons_Comm_Code):
    global code_mapping_df
    Cons_code_mapping_df = code_mapping_df.copy(deep=True)
    Cons_code_mapping_df.reset_index()
    Cons_code_mapping_df.set_index("Consolidated Level Commodity Code - 22 level", inplace=True)
    temp_extract = Cons_code_mapping_df.loc[Cons_Comm_Code,"Consolidated Commodity Code description"][0]
    if len(temp_extract) == 1:
        temp_extract = Cons_code_mapping_df.loc[Cons_Comm_Code,"Consolidated Commodity Code description"]
    comm_code_extract = {"Consolidated_Comm_Code": str(Cons_Comm_Code), "Consolidated_Comm_Code_desc": temp_extract} 
    comm_code_JSON = json.dumps(comm_code_extract)
    return comm_code_JSON


## Read the excel user input dashboard
def Dashboard_Readin(UserInput):
    print ("Recieved the UserINput file")
    global dashboard_userinput_df, Year, Year_Adjustment_df, Prod_Purch_adj_df, commodities_list, Purchased_Commodities_list
    
    commodities_list = []
    
    # Reading and cleaning the User input data
    temp_dashboard = UserInput
    Year = int(temp_dashboard.loc[0,'Year'])
    temp_dashboard.drop(["Year"],axis = 1, inplace=True)
    
    # Getting all the User Input commodity codes into a list
    temp_list = temp_dashboard["Commodity Code"].tolist()
    for each_code in temp_list:
        if each_code not in commodities_list:
            commodities_list.append(each_code)
            
    # Getting the list of commodity codes whose value is "Purchased" into a list    
    temp_Purchased_List = temp_dashboard[temp_dashboard["Producer/Purchaser"]== "Purchased"]["Commodity Code"].tolist()
    for each_comm in temp_Purchased_List:
        if each_comm not in Purchased_Commodities_list:
            Purchased_Commodities_list.append(each_comm)    
            
    dashboard_userinput_df = temp_dashboard.copy(deep=True)
    dashboard_userinput_df.set_index("Commodities",inplace=True)
    
    Calculations() ## Perform the model calculations based on the 411x411 matrix and detail model outputs files.
    Scope3_Breakdown() ## Performs the Scope 3 breadown and saves it at detailed level
    Scope3_Breakdown_SummaryLevel() ## Saves the Scope 3 breakdown in the summary level format
    Scope3_Breakdown_ConsolidatedLevel() ## Saves the Scope 3 breakdown in the Consolidated level format
    Analysis_Tableau() ## Formats the model outputs in the fixed format which serves as an input to Tableau.
    Tier_Chart()
    Tier_Commodities()
    Analysis2() ## To save the outputs of the analysis in a fixed readable format
    return SaveJSONfiles()


#***************** SECTION 3: Performing the analysis *****************
#######################################
# FileReadin
# Purpose           : To perform the analysis to give the scope 3 breakdown of company all-together.
#######################################


def Calculations():
    print ("Dashboard Calculations in progress..")
    # Filter the 411x411 dataset with commodity codes that user is interested
    global calculations_df, Full_data_411_411_df, commodities_list, Purchased_Commodities_list
    calculations_df = Full_data_411_411_df.copy()[Full_data_411_411_df.index.isin(commodities_list)]
    calculations_df.reset_index(inplace=True)
    # Adding a col for Year adjustment factor
    for i, row in calculations_df.iterrows():
        ## Year_adj
        temp_Yr_adj = json.loads(Get_Year_adj_Value(str(calculations_df.loc[i,"OP_Comm"]),Year))
        calculations_df.loc[i,"Yr_adj"] = temp_Yr_adj["Year_Adj"]

        ## Producer/Purchaser adj, only if user enters "Purchased" for a commodity code else the adj value remains 1.
        if i in Purchased_Commodities_list:
            temp_Pr_adj = json.loads(Get_Prod_Purchaser_Value(str(calculations_df.loc[i,"OP_Comm"]),Year))
            calculations_df.loc[i,"Pro_Pur_adj"] = temp_Pr_adj["Pro_Pur_adj"]
        else:
            calculations_df.loc[i,"Pro_Pur_adj"] = 1

        ## Adjusted TR x DI = TRxDI * Year_adj * Pro_Pur_adj
        ## Adjusted DR x TI = DRxTI * Year_adj * Pro_Pur_adj
        calculations_df.loc[i,"Adj_TRxDI"] = calculations_df.loc[i,"TRxDI"] * calculations_df.loc[i,"Yr_adj"] * calculations_df.loc[i,"Pro_Pur_adj"]
        
        calculations_df.loc[i,"Adj_DRxTI"] = calculations_df.loc[i,"DRxTI"] * calculations_df.loc[i,"Yr_adj"] * calculations_df.loc[i,"Pro_Pur_adj"]
        calculations_df.loc[i,"Adj_DRxDI"] = calculations_df.loc[i,"DRxDI"] * calculations_df.loc[i,"Yr_adj"] * calculations_df.loc[i,"Pro_Pur_adj"]
        
        ## Revenue
        temp_revenue = json.loads(Get_Revenue_Summary_Value(str(calculations_df.loc[i,"OP_Comm"])))
        calculations_df.loc[i,"Revenue"] = temp_revenue["Revenue"]

        ## Final TRxDI = Adjusted TR x DI * Revenue
        ## Final DRxTI = Adjusted DR x TI * Revenue
        calculations_df.loc[i,"Scope 3 kg(TR)"] =  calculations_df.loc[i,"Adj_TRxDI"]  * calculations_df.loc[i,"Revenue"]
        calculations_df.loc[i,"Scope 3 kg(DR)"] =  calculations_df.loc[i,"Adj_DRxTI"] * calculations_df.loc[i,"Revenue"] 
        calculations_df.loc[i,"DR S1"] =  calculations_df.loc[i,"Adj_DRxDI"] * calculations_df.loc[i,"Revenue"]
        calculations_df.loc[i,"Tier 2->N S1"] = calculations_df.loc[i,"Scope 3 kg(TR)"]-calculations_df.loc[i,"DR S1"] 
    return

## Scope 3 breakdown of the company over all at Detail level
def Scope3_Breakdown():
    global calculations_df, Scope3_Breakdown_df, Scope3_Breakdown_df_DR_Sorted
    temp_Scope3_Breakdown = calculations_df[['DR', 'IP_Comm', 'Scope 3 kg(TR)', 'Scope 3 kg(DR)', 'DR S1', 'Tier 2->N S1']].copy()
    Scope3_Breakdown_df = pd.pivot_table(temp_Scope3_Breakdown,index=["IP_Comm"], aggfunc = np.sum)
    Scope3_Breakdown_df.reset_index(inplace=True)
    for m, row in Scope3_Breakdown_df.iterrows():
        # Commodtiy Code name
        temp_Comm_Name = json.loads(Get_CodeMapping_Value(str(Scope3_Breakdown_df.loc[m,"IP_Comm"])))
        Scope3_Breakdown_df.loc[m,"Description"] = temp_Comm_Name['Detail_Comm_Code_desc']
        Scope3_Breakdown_df.loc[m,"Summary_Comm_Code"] =  temp_Comm_Name['Summary_Comm_Code']
        Scope3_Breakdown_df.loc[m,"Consolidated_Comm_Code"] =  temp_Comm_Name['Consolidated_Comm_Code']
        Scope3_Breakdown_df.loc[m,"% of DR"] = Scope3_Breakdown_df.loc[m,"DR"]*100 / sum(Scope3_Breakdown_df['DR'])
        # % Scope 3 (DR) and % Scope 3 (TR)
        Scope3_Breakdown_df.loc[m,"% Scope 3 (TR)"] = Scope3_Breakdown_df.loc[m,"Scope 3 kg(TR)"]*100 / sum(Scope3_Breakdown_df['Scope 3 kg(TR)'])
        Scope3_Breakdown_df.loc[m,"% Scope 3 (DR)"] = Scope3_Breakdown_df.loc[m,"Scope 3 kg(DR)"]*100 / sum(Scope3_Breakdown_df['Scope 3 kg(DR)'])
        Scope3_Breakdown_df.loc[m,"% of DR S1"] = Scope3_Breakdown_df.loc[m,"DR S1"]*100 / sum(Scope3_Breakdown_df['Scope 3 kg(DR)'])
        Scope3_Breakdown_df.loc[m,"% Tier 2->N S1"] = Scope3_Breakdown_df.loc[m,"Tier 2->N S1"]*100 / sum(Scope3_Breakdown_df['Scope 3 kg(DR)'])
        Scope3_Breakdown_df.loc[m,"% S1"] = Model_OP_411_df.loc[Scope3_Breakdown_df.loc[m,"IP_Comm"],"Percent_Scope1"]
        Scope3_Breakdown_df.loc[m,"% S2"] = Model_OP_411_df.loc[Scope3_Breakdown_df.loc[m,"IP_Comm"],"Percent_Scope2"]
        Scope3_Breakdown_df.loc[m,"% S3"] = Model_OP_411_df.loc[Scope3_Breakdown_df.loc[m,"IP_Comm"],"Percent_Scope3"]
        if Scope3_Breakdown_df.loc[m,"IP_Comm"] == "221100/US":
            Scope3_Breakdown_df.loc[m,"DR x S1"] = 0
        else:
            Scope3_Breakdown_df.loc[m,"DR x S1"] = (Scope3_Breakdown_df.loc[m,"% S1"]*Scope3_Breakdown_df.loc[m,"% Scope 3 (DR)"])/100
        Scope3_Breakdown_df.loc[m,"DR x S2"] = (Scope3_Breakdown_df.loc[m,"% S2"]*Scope3_Breakdown_df.loc[m,"% Scope 3 (DR)"])/100
        Scope3_Breakdown_df.loc[m,"DR x S3"] = (Scope3_Breakdown_df.loc[m,"% S3"]*Scope3_Breakdown_df.loc[m,"% Scope 3 (DR)"])/100
        

    Scope3_Breakdown_df['TR S3 Rank']= Scope3_Breakdown_df['Scope 3 kg(TR)'].rank(method='min',ascending=False)
    Scope3_Breakdown_df['DR S3 Rank']= Scope3_Breakdown_df['Scope 3 kg(DR)'].rank(method='min',ascending=False)
    Scope3_Breakdown_df['Ratio'] = Scope3_Breakdown_df['% of DR S1']/Scope3_Breakdown_df['% Tier 2->N S1']
    
    for n, row in Scope3_Breakdown_df.iterrows():
        
        if Scope3_Breakdown_df.loc[n, "Ratio"] > 1:
            Scope3_Breakdown_df.loc[n, "Visibility"] = "High"
        else: Scope3_Breakdown_df.loc[n, "Visibility"] = "Low"
        
        if Scope3_Breakdown_df.loc[n, "% of DR"] > 2:
            Scope3_Breakdown_df.loc[n, "Influence"] = "High"
        else: Scope3_Breakdown_df.loc[n, "Influence"] = "Low"
        
        if Scope3_Breakdown_df.loc[n, "% Scope 3 (TR)"] > 2:
            Scope3_Breakdown_df.loc[n, "Significance"] = "High"
        else: Scope3_Breakdown_df.loc[n, "Significance"] = "Low"
        
        if Scope3_Breakdown_df.loc[n, "Visibility"] == "High" and Scope3_Breakdown_df.loc[n, "Influence"] == "High" and Scope3_Breakdown_df.loc[n, "Significance"] == "High":
            Scope3_Breakdown_df.loc[n, "Strategy"] = "Supplier Engagement"
            
        elif Scope3_Breakdown_df.loc[n, "Visibility"] == "High" or (Scope3_Breakdown_df.loc[n, "Influence"] == "High" and Scope3_Breakdown_df.loc[n, "Significance"] == "High"):
            Scope3_Breakdown_df.loc[n, "Strategy"] = "Industry Collaboration"
        
        else:
            Scope3_Breakdown_df.loc[n, "Strategy"] = "Ecosystem Activation"
        
    Scope3_Breakdown_df = Scope3_Breakdown_df[['IP_Comm','Description','DR','% of DR', 'TR S3 Rank', 'DR S3 Rank', 'Scope 3 kg(TR)', 'Scope 3 kg(DR)', 'DR S1', 'Tier 2->N S1', '% Scope 3 (TR)', '% Scope 3 (DR)', '% of DR S1', '% Tier 2->N S1', 'DR x S1','DR x S2','DR x S3', 'Summary_Comm_Code', 'Consolidated_Comm_Code', 'Strategy']]
    Scope3_Breakdown_df.sort_values(by="TR S3 Rank",ascending=True,inplace=True)
    
    Scope3_Breakdown_df_DR_Sorted = Scope3_Breakdown_df.copy(deep=True)
    Scope3_Breakdown_df_DR_Sorted.sort_values(by="DR S3 Rank",ascending=True,inplace=True)

    print ("Scope 3 Break down analysis is done!")
    return

## Scope 3 breakdown of the company over all at Summary level
def Scope3_Breakdown_SummaryLevel():
    global Scope3_Breakdown_df, Scope3_Breakdown_Summary_df, Scope3_Breakdown_Summary_df_DR_Sorted
    temp_S3_Breakdown_df = Scope3_Breakdown_df[['Summary_Comm_Code','DR','Scope 3 kg(DR)','Scope 3 kg(TR)','DR S1','Tier 2->N S1']].copy()
    Scope3_Breakdown_Summary_df = pd.pivot_table(temp_S3_Breakdown_df,index=["Summary_Comm_Code"], aggfunc = np.sum)
    Scope3_Breakdown_Summary_df.reset_index(inplace=True)
    for m, row in Scope3_Breakdown_Summary_df.iterrows():
        # % Scope 3 (DR) and % Scope 3 (TR)
        temp_Comm_Name = json.loads(Get_CodeMapping_Value_Summary(Scope3_Breakdown_Summary_df.loc[m,"Summary_Comm_Code"]))
        Scope3_Breakdown_Summary_df.loc[m,"Description"]  = temp_Comm_Name['Summary_Comm_Code_desc']
        Scope3_Breakdown_Summary_df.loc[m,"% Scope 3 (TR)"] = Scope3_Breakdown_Summary_df.loc[m,"Scope 3 kg(TR)"]*100 / sum(Scope3_Breakdown_Summary_df['Scope 3 kg(TR)'])
        Scope3_Breakdown_Summary_df.loc[m,"% Scope 3 (DR)"] = Scope3_Breakdown_Summary_df.loc[m,"Scope 3 kg(DR)"]*100 / sum(Scope3_Breakdown_Summary_df['Scope 3 kg(DR)'])
        Scope3_Breakdown_Summary_df.loc[m,"% of DR"] = Scope3_Breakdown_Summary_df.loc[m,"DR"]*100 / sum(Scope3_Breakdown_Summary_df['DR'])
        Scope3_Breakdown_Summary_df.loc[m,"% of DR S1"] = Scope3_Breakdown_Summary_df.loc[m,"DR S1"]*100 / sum(Scope3_Breakdown_Summary_df['Scope 3 kg(TR)'])
        Scope3_Breakdown_Summary_df.loc[m,"% of Tier 2->N S1"] = Scope3_Breakdown_Summary_df.loc[m,"Tier 2->N S1"]*100 / sum(Scope3_Breakdown_Summary_df['Scope 3 kg(TR)'])
        Scope3_Breakdown_Summary_df.loc[m,"% S1"] = Model_OP_92_df.loc[temp_Comm_Name['Summary_Comm_Code_desc'],"Percent_Scope1"]
        Scope3_Breakdown_Summary_df.loc[m,"% S2"] = Model_OP_92_df.loc[temp_Comm_Name['Summary_Comm_Code_desc'],"Percent_Scope2"]
        Scope3_Breakdown_Summary_df.loc[m,"% S3"] = Model_OP_92_df.loc[temp_Comm_Name['Summary_Comm_Code_desc'],"Percent_Scope3"]
        Scope3_Breakdown_Summary_df.loc[m,"DR x S1"] = (Scope3_Breakdown_Summary_df.loc[m,"% S1"]*Scope3_Breakdown_Summary_df.loc[m,"% Scope 3 (DR)"])/100
        Scope3_Breakdown_Summary_df.loc[m,"DR x S2"] = (Scope3_Breakdown_Summary_df.loc[m,"% S2"]*Scope3_Breakdown_Summary_df.loc[m,"% Scope 3 (DR)"])/100
        Scope3_Breakdown_Summary_df.loc[m,"DR x S3"] = (Scope3_Breakdown_Summary_df.loc[m,"% S3"]*Scope3_Breakdown_Summary_df.loc[m,"% Scope 3 (DR)"])/100
    Scope3_Breakdown_Summary_df['TR S3 Rank']= Scope3_Breakdown_Summary_df['Scope 3 kg(TR)'].rank(method='min',ascending=False)
    Scope3_Breakdown_Summary_df['DR S3 Rank']= Scope3_Breakdown_Summary_df['Scope 3 kg(DR)'].rank(method='min',ascending=False)
    Scope3_Breakdown_Summary_df['Ratio'] = Scope3_Breakdown_Summary_df['% of DR S1']/Scope3_Breakdown_Summary_df['% of Tier 2->N S1']
    
    for n, row in Scope3_Breakdown_Summary_df.iterrows():
        
        if Scope3_Breakdown_Summary_df.loc[n, "Ratio"] > 1:
            Scope3_Breakdown_Summary_df.loc[n, "Visibility"] = "High"
        else: Scope3_Breakdown_Summary_df.loc[n, "Visibility"] = "Low"
        
        if Scope3_Breakdown_Summary_df.loc[n, "% of DR"] > 4:
            Scope3_Breakdown_Summary_df.loc[n, "Influence"] = "High"
        else: Scope3_Breakdown_Summary_df.loc[n, "Influence"] = "Low"
        
        if Scope3_Breakdown_Summary_df.loc[n, "% Scope 3 (TR)"] > 4:
            Scope3_Breakdown_Summary_df.loc[n, "Significance"] = "High"
        else: Scope3_Breakdown_Summary_df.loc[n, "Significance"] = "Low"
        
        if Scope3_Breakdown_Summary_df.loc[n, "Visibility"] == "High" and Scope3_Breakdown_Summary_df.loc[n, "Influence"] == "High" and Scope3_Breakdown_Summary_df.loc[n, "Significance"] == "High":
            Scope3_Breakdown_Summary_df.loc[n, "Strategy"] = "Supplier Engagement"
            
        elif Scope3_Breakdown_Summary_df.loc[n, "Visibility"] == "High" or (Scope3_Breakdown_Summary_df.loc[n, "Influence"] == "High" and Scope3_Breakdown_Summary_df.loc[n, "Significance"] == "High"):
            Scope3_Breakdown_Summary_df.loc[n, "Strategy"] = "Industry Collaboration"
        
        else:
            Scope3_Breakdown_Summary_df.loc[n, "Strategy"] = "Ecosystem Activation"
            
    Scope3_Breakdown_Summary_df = Scope3_Breakdown_Summary_df[['Summary_Comm_Code','Description','DR','% of DR', 'Scope 3 kg(TR)', 'Scope 3 kg(DR)','% Scope 3 (TR)', '% Scope 3 (DR)','DR S1','Tier 2->N S1','% of DR S1','% of Tier 2->N S1', 'DR x S1','DR x S2','DR x S3','TR S3 Rank', 'DR S3 Rank', 'Strategy']]
    Scope3_Breakdown_Summary_df.sort_values(by="TR S3 Rank",ascending=True,inplace=True)
    
    Scope3_Breakdown_Summary_df_DR_Sorted = Scope3_Breakdown_Summary_df.copy(deep=True)
    Scope3_Breakdown_Summary_df_DR_Sorted.sort_values(by="DR S3 Rank",ascending=True,inplace=True)  
    
    print ("Scope 3 Break down analysis - Summary level is done!")



## Scope 3 breakdown of the company over all at Scope3_Breakdown_ConsolidatedLevel
def Scope3_Breakdown_ConsolidatedLevel():
    global Scope3_Breakdown_df, Scope3_Breakdown_Consolidated_df, Scope3_Breakdown_Consolidated_df_DR_Sorted
    temp_S3_Breakdown_df = Scope3_Breakdown_df[['Consolidated_Comm_Code','DR','Scope 3 kg(DR)','Scope 3 kg(TR)','DR S1','Tier 2->N S1']].copy()
    Scope3_Breakdown_Consolidated_df = pd.pivot_table(temp_S3_Breakdown_df,index=["Consolidated_Comm_Code"], aggfunc = np.sum)
    Scope3_Breakdown_Consolidated_df.reset_index(inplace=True)
    for m, row in Scope3_Breakdown_Consolidated_df.iterrows():
        # % Scope 3 (DR) and % Scope 3 (TR)
        temp_Comm_Name = json.loads(Get_CodeMapping_Value_Cons(str(Scope3_Breakdown_Consolidated_df.loc[m,"Consolidated_Comm_Code"])))
        Scope3_Breakdown_Consolidated_df.loc[m,"Description"]  = temp_Comm_Name['Consolidated_Comm_Code_desc']
        Scope3_Breakdown_Consolidated_df.loc[m,"% Scope 3 (TR)"] = Scope3_Breakdown_Consolidated_df.loc[m,"Scope 3 kg(TR)"]*100 / sum(Scope3_Breakdown_Consolidated_df['Scope 3 kg(TR)'])
        Scope3_Breakdown_Consolidated_df.loc[m,"% Scope 3 (DR)"] = Scope3_Breakdown_Consolidated_df.loc[m,"Scope 3 kg(DR)"]*100 / sum(Scope3_Breakdown_Consolidated_df['Scope 3 kg(DR)'])
        Scope3_Breakdown_Consolidated_df.loc[m,"% of DR"] = Scope3_Breakdown_Consolidated_df.loc[m,"DR"]*100 / sum(Scope3_Breakdown_Consolidated_df['DR'])
        Scope3_Breakdown_Consolidated_df.loc[m,"% of DR S1"] = Scope3_Breakdown_Consolidated_df.loc[m,"DR S1"]*100 / sum(Scope3_Breakdown_Consolidated_df['Scope 3 kg(TR)'])
        Scope3_Breakdown_Consolidated_df.loc[m,"% of Tier 2->N S1"] = Scope3_Breakdown_Consolidated_df.loc[m,"Tier 2->N S1"]*100 / sum(Scope3_Breakdown_Consolidated_df['Scope 3 kg(TR)'])
        Scope3_Breakdown_Consolidated_df.loc[m,"% S1"] = Model_OP_22_df.loc[temp_Comm_Name['Consolidated_Comm_Code_desc'],"Percent_Scope1"]
        Scope3_Breakdown_Consolidated_df.loc[m,"% S2"] = Model_OP_22_df.loc[temp_Comm_Name['Consolidated_Comm_Code_desc'],"Percent_Scope2"]
        Scope3_Breakdown_Consolidated_df.loc[m,"% S3"] = Model_OP_22_df.loc[temp_Comm_Name['Consolidated_Comm_Code_desc'],"Percent_Scope3"]
        Scope3_Breakdown_Consolidated_df.loc[m,"DR x S1"] = (Scope3_Breakdown_Consolidated_df.loc[m,"% S1"]*Scope3_Breakdown_Summary_df.loc[m,"% Scope 3 (DR)"])/100
        Scope3_Breakdown_Consolidated_df.loc[m,"DR x S2"] = (Scope3_Breakdown_Consolidated_df.loc[m,"% S2"]*Scope3_Breakdown_Summary_df.loc[m,"% Scope 3 (DR)"])/100
        Scope3_Breakdown_Consolidated_df.loc[m,"DR x S3"] = (Scope3_Breakdown_Consolidated_df.loc[m,"% S3"]*Scope3_Breakdown_Summary_df.loc[m,"% Scope 3 (DR)"])/100
    Scope3_Breakdown_Consolidated_df['TR S3 Rank']= Scope3_Breakdown_Consolidated_df['Scope 3 kg(TR)'].rank(method='min',ascending=False)
    Scope3_Breakdown_Consolidated_df['DR S3 Rank']= Scope3_Breakdown_Consolidated_df['Scope 3 kg(DR)'].rank(method='min',ascending=False)
    Scope3_Breakdown_Consolidated_df['Ratio'] = Scope3_Breakdown_Consolidated_df['% of DR S1']/Scope3_Breakdown_Consolidated_df['% of Tier 2->N S1']
    
    for n, row in Scope3_Breakdown_Consolidated_df.iterrows():
        
        if Scope3_Breakdown_Consolidated_df.loc[n, "Ratio"] > 1:
            Scope3_Breakdown_Consolidated_df.loc[n, "Visibility"] = "High"
        else: Scope3_Breakdown_Consolidated_df.loc[n, "Visibility"] = "Low"
        
        if Scope3_Breakdown_Consolidated_df.loc[n, "% of DR"] > 4:
            Scope3_Breakdown_Consolidated_df.loc[n, "Influence"] = "High"
        else: Scope3_Breakdown_Consolidated_df.loc[n, "Influence"] = "Low"
        
        if Scope3_Breakdown_Consolidated_df.loc[n, "% Scope 3 (TR)"] > 4:
            Scope3_Breakdown_Consolidated_df.loc[n, "Significance"] = "High"
        else: Scope3_Breakdown_Consolidated_df.loc[n, "Significance"] = "Low"
        
        if Scope3_Breakdown_Consolidated_df.loc[n, "Visibility"] == "High" and Scope3_Breakdown_Consolidated_df.loc[n, "Influence"] == "High" and Scope3_Breakdown_Consolidated_df.loc[n, "Significance"] == "High":
            Scope3_Breakdown_Consolidated_df.loc[n, "Strategy"] = "Supplier Engagement"
            
        elif Scope3_Breakdown_Consolidated_df.loc[n, "Visibility"] == "High" or (Scope3_Breakdown_Consolidated_df.loc[n, "Influence"] == "High" and Scope3_Breakdown_Consolidated_df.loc[n, "Significance"] == "High"):
            Scope3_Breakdown_Consolidated_df.loc[n, "Strategy"] = "Industry Collaboration"
        
        else:
            Scope3_Breakdown_Consolidated_df.loc[n, "Strategy"] = "Ecosystem Activation"
    
    Scope3_Breakdown_Consolidated_df = Scope3_Breakdown_Consolidated_df[['Consolidated_Comm_Code','Description','DR','% of DR', 'Scope 3 kg(TR)', 'Scope 3 kg(DR)','% Scope 3 (TR)', '% Scope 3 (DR)','DR S1','Tier 2->N S1','% of DR S1','% of Tier 2->N S1','DR x S1','DR x S2','DR x S3','TR S3 Rank', 'DR S3 Rank', 'Strategy']]
    Scope3_Breakdown_Consolidated_df.sort_values(by="TR S3 Rank",ascending=True,inplace=True)
    
    Scope3_Breakdown_Consolidated_df_DR_Sorted = Scope3_Breakdown_Consolidated_df.copy(deep=True)
    Scope3_Breakdown_Consolidated_df_DR_Sorted.sort_values(by="DR S3 Rank",ascending=True,inplace=True)  
    
    print ("Scope 3 Break down analysis - Consolidated level is done!")
    
def Tier_Chart():
    global Tier_Chart_df
    global Complete_Tier_Chart_df
    Tier_Chart_df = Analysis_Tableau_df.copy()
    Tier_Chart_df.reset_index(inplace=True)
    
    for j, row in Tier_Chart_df.iterrows():
         Tier_Chart_df.loc[j, "Adjusted Revenue"] = Tier_Chart_df.loc[j, "Revenue ($)"]*Tier_Chart_df.loc[j, "Year Price adjustment"]
         
         #get S3 percentages (adding up to 100%) for each tier
         Tier_Chart_df.loc[j, "Company S1 Label"] = Tier_Percentages_df.loc[Tier_Chart_df.loc[j,"Commodity Code"],"T1 S1 %"]
         Tier_Chart_df.loc[j, "Company S2 Label"] = Tier_Percentages_df.loc[Tier_Chart_df.loc[j,"Commodity Code"],"T1 S2 %"]
         Tier_Chart_df.loc[j, "Company S3 Label"] = Tier_Percentages_df.loc[Tier_Chart_df.loc[j,"Commodity Code"],"T1 S3 %"]
         Tier_Chart_df.loc[j, "T1 S1 Label"] = Tier_Percentages_df.loc[Tier_Chart_df.loc[j,"Commodity Code"],"T2 S1 %"]
         Tier_Chart_df.loc[j, "T1 S2 Label"] = Tier_Percentages_df.loc[Tier_Chart_df.loc[j,"Commodity Code"],"T2 S2 %"]
         Tier_Chart_df.loc[j, "T1 S3 Label"] = Tier_Percentages_df.loc[Tier_Chart_df.loc[j,"Commodity Code"],"T2 S3 %"]
         Tier_Chart_df.loc[j, "T2 S1 Label"] = Tier_Percentages_df.loc[Tier_Chart_df.loc[j,"Commodity Code"],"T3 S1 %"]
         Tier_Chart_df.loc[j, "T2 S2 Label"] = Tier_Percentages_df.loc[Tier_Chart_df.loc[j,"Commodity Code"],"T3 S2 %"]
         Tier_Chart_df.loc[j, "T2 S3 Label"] = Tier_Percentages_df.loc[Tier_Chart_df.loc[j,"Commodity Code"],"T3 S3 %"]
         Tier_Chart_df.loc[j, "T3 S1 Label"] = Tier_Percentages_df.loc[Tier_Chart_df.loc[j,"Commodity Code"],"T4 S1 %"]
         Tier_Chart_df.loc[j, "T3 S2 Label"] = Tier_Percentages_df.loc[Tier_Chart_df.loc[j,"Commodity Code"],"T4 S2 %"]
         Tier_Chart_df.loc[j, "T3 S3 Label"] = Tier_Percentages_df.loc[Tier_Chart_df.loc[j,"Commodity Code"],"T4 S3 %"]
         
         #calculate breakdown of each tear of S3
         Tier_Chart_df.loc[j, "Company S1 %"] = Tier_Chart_df.loc[j, "Company S1 Label"]
         Tier_Chart_df.loc[j, "Company S2 %"] = Tier_Chart_df.loc[j, "Company S2 Label"]
         Tier_Chart_df.loc[j, "Company S3 %"] = Tier_Chart_df.loc[j, "Company S3 Label"]
         Tier_Chart_df.loc[j, "T1 S1 %"] = Tier_Chart_df.loc[j, "T1 S1 Label"]*Tier_Chart_df.loc[j, "Company S3 %"]     
         Tier_Chart_df.loc[j, "T1 S2 %"] = Tier_Chart_df.loc[j, "T1 S2 Label"]*Tier_Chart_df.loc[j, "Company S3 %"]
         Tier_Chart_df.loc[j, "T1 S3 %"] = Tier_Chart_df.loc[j, "T1 S3 Label"]*Tier_Chart_df.loc[j, "Company S3 %"]
         Tier_Chart_df.loc[j, "T2 S1 %"] = Tier_Chart_df.loc[j, "T2 S1 Label"]*Tier_Chart_df.loc[j, "T1 S3 %"]
         Tier_Chart_df.loc[j, "T2 S2 %"] = Tier_Chart_df.loc[j, "T2 S2 Label"]*Tier_Chart_df.loc[j, "T1 S3 %"]                            
         Tier_Chart_df.loc[j, "T2 S3 %"] = Tier_Chart_df.loc[j, "T2 S3 Label"]*Tier_Chart_df.loc[j, "T1 S3 %"] 
         Tier_Chart_df.loc[j, "T3 S1 %"] = Tier_Chart_df.loc[j, "T3 S1 Label"]*Tier_Chart_df.loc[j, "T2 S3 %"]
         Tier_Chart_df.loc[j, "T3 S2 %"] = Tier_Chart_df.loc[j, "T3 S2 Label"]*Tier_Chart_df.loc[j, "T2 S3 %"]                              
         Tier_Chart_df.loc[j, "T3 S3 %"] = Tier_Chart_df.loc[j, "T3 S3 Label"]*Tier_Chart_df.loc[j, "T2 S3 %"]
         
         #get transaction counts at each tier
         Tier_Chart_df.loc[j, "T1 Count"] = Tier_Counts_df.loc[Tier_Chart_df.loc[j,"Commodity Code"],"Tier 1"]
         Tier_Chart_df.loc[j, "T2 Count"] = Tier_Counts_df.loc[Tier_Chart_df.loc[j,"Commodity Code"],"Tier 2"]
         Tier_Chart_df.loc[j, "T3 Count"] = Tier_Counts_df.loc[Tier_Chart_df.loc[j,"Commodity Code"],"Tier 3"]
         Tier_Chart_df.loc[j, "T4 Count"] = Tier_Counts_df.loc[Tier_Chart_df.loc[j,"Commodity Code"],"Tier 4"]
    
    for k, row in Tier_Chart_df.iterrows():      
        Tier_Chart_df.loc[k, "% of Revenue"] = Tier_Chart_df.loc[k, "Adjusted Revenue"]/sum(Tier_Chart_df["Adjusted Revenue"])

    #calculate revenue-weighted emissions for each tier for each LOB
    for l, row in Tier_Chart_df.iterrows():
        Tier_Chart_df.loc[l, "Weighted Company S1 %"] = Tier_Chart_df.loc[l,"Company S1 %"]*Tier_Chart_df.loc[l,"% of Revenue"]
        Tier_Chart_df.loc[l, "Weighted Company S2 %"] = Tier_Chart_df.loc[l,"Company S2 %"]*Tier_Chart_df.loc[l,"% of Revenue"]
        Tier_Chart_df.loc[l, "Weighted Company S3 %"] = Tier_Chart_df.loc[l,"Company S3 %"]*Tier_Chart_df.loc[l,"% of Revenue"]
        Tier_Chart_df.loc[l, "Weighted T1 S1 %"] = Tier_Chart_df.loc[l,"T1 S1 %"]*Tier_Chart_df.loc[l,"% of Revenue"]
        Tier_Chart_df.loc[l, "Weighted T1 S2 %"] = Tier_Chart_df.loc[l,"T1 S2 %"]*Tier_Chart_df.loc[l,"% of Revenue"]
        Tier_Chart_df.loc[l, "Weighted T1 S3 %"] = Tier_Chart_df.loc[l,"T1 S3 %"]*Tier_Chart_df.loc[l,"% of Revenue"]
        Tier_Chart_df.loc[l, "Weighted T2 S1 %"] = Tier_Chart_df.loc[l,"T2 S1 %"]*Tier_Chart_df.loc[l,"% of Revenue"]
        Tier_Chart_df.loc[l, "Weighted T2 S2 %"] = Tier_Chart_df.loc[l,"T2 S2 %"]*Tier_Chart_df.loc[l,"% of Revenue"]
        Tier_Chart_df.loc[l, "Weighted T2 S3 %"] = Tier_Chart_df.loc[l,"T2 S3 %"]*Tier_Chart_df.loc[l,"% of Revenue"]
        Tier_Chart_df.loc[l, "Weighted T3 S1 %"] = Tier_Chart_df.loc[l,"T3 S1 %"]*Tier_Chart_df.loc[l,"% of Revenue"]
        Tier_Chart_df.loc[l, "Weighted T3 S2 %"] = Tier_Chart_df.loc[l,"T3 S2 %"]*Tier_Chart_df.loc[l,"% of Revenue"]
        Tier_Chart_df.loc[l, "Weighted T3 S3 %"] = Tier_Chart_df.loc[l,"T3 S3 %"]*Tier_Chart_df.loc[l,"% of Revenue"]
        
        Tier_Chart_df.loc[l, "Weighted Company S1 Label"] = Tier_Chart_df.loc[l,"Company S1 Label"]*Tier_Chart_df.loc[l,"% of Revenue"]
        Tier_Chart_df.loc[l, "Weighted Company S2 Label"] = Tier_Chart_df.loc[l,"Company S2 Label"]*Tier_Chart_df.loc[l,"% of Revenue"]
        Tier_Chart_df.loc[l, "Weighted Company S3 Label"] = Tier_Chart_df.loc[l,"Company S3 Label"]*Tier_Chart_df.loc[l,"% of Revenue"]
        Tier_Chart_df.loc[l, "Weighted T1 S1 Label"] = Tier_Chart_df.loc[l,"T1 S1 Label"]*Tier_Chart_df.loc[l,"% of Revenue"]
        Tier_Chart_df.loc[l, "Weighted T1 S2 Label"] = Tier_Chart_df.loc[l,"T1 S2 Label"]*Tier_Chart_df.loc[l,"% of Revenue"]
        Tier_Chart_df.loc[l, "Weighted T1 S3 Label"] = Tier_Chart_df.loc[l,"T1 S3 Label"]*Tier_Chart_df.loc[l,"% of Revenue"]
        Tier_Chart_df.loc[l, "Weighted T2 S1 Label"] = Tier_Chart_df.loc[l,"T2 S1 Label"]*Tier_Chart_df.loc[l,"% of Revenue"]
        Tier_Chart_df.loc[l, "Weighted T2 S2 Label"] = Tier_Chart_df.loc[l,"T2 S2 Label"]*Tier_Chart_df.loc[l,"% of Revenue"]
        Tier_Chart_df.loc[l, "Weighted T2 S3 Label"] = Tier_Chart_df.loc[l,"T2 S3 Label"]*Tier_Chart_df.loc[l,"% of Revenue"]
        Tier_Chart_df.loc[l, "Weighted T3 S1 Label"] = Tier_Chart_df.loc[l,"T3 S1 Label"]*Tier_Chart_df.loc[l,"% of Revenue"]
        Tier_Chart_df.loc[l, "Weighted T3 S2 Label"] = Tier_Chart_df.loc[l,"T3 S2 Label"]*Tier_Chart_df.loc[l,"% of Revenue"]
        Tier_Chart_df.loc[l, "Weighted T3 S3 Label"] = Tier_Chart_df.loc[l,"T3 S3 Label"]*Tier_Chart_df.loc[l,"% of Revenue"]
        
        Tier_Chart_df.loc[l, "Weighted T1 Count"] = Tier_Chart_df.loc[l,"T1 Count"]*Tier_Chart_df.loc[l,"% of Revenue"]
        Tier_Chart_df.loc[l, "Weighted T2 Count"] = Tier_Chart_df.loc[l,"T2 Count"]*Tier_Chart_df.loc[l,"% of Revenue"]
        Tier_Chart_df.loc[l, "Weighted T3 Count"] = Tier_Chart_df.loc[l,"T3 Count"]*Tier_Chart_df.loc[l,"% of Revenue"]
        Tier_Chart_df.loc[l, "Weighted T4 Count"] = Tier_Chart_df.loc[l,"T4 Count"]*Tier_Chart_df.loc[l,"% of Revenue"]
    
    Tier_Chart_df["Company S1 Label"] = Tier_Chart_df["Company S1 Label"]*100
    Tier_Chart_df["Company S2 Label"] = Tier_Chart_df["Company S2 Label"]*100
    Tier_Chart_df["Company S3 Label"] = Tier_Chart_df["Company S3 Label"]*100
    Tier_Chart_df["T1 S1 Label"] = Tier_Chart_df["T1 S1 Label"]*100
    Tier_Chart_df["T1 S2 Label"] = Tier_Chart_df["T1 S2 Label"]*100
    Tier_Chart_df["T1 S3 Label"] = Tier_Chart_df["T1 S3 Label"]*100
    Tier_Chart_df["T2 S1 Label"] = Tier_Chart_df["T2 S1 Label"]*100
    Tier_Chart_df["T2 S2 Label"] = Tier_Chart_df["T2 S2 Label"]*100
    Tier_Chart_df["T2 S3 Label"] = Tier_Chart_df["T2 S3 Label"]*100
    Tier_Chart_df["T3 S1 Label"] = Tier_Chart_df["T3 S1 Label"]*100
    Tier_Chart_df["T3 S2 Label"] = Tier_Chart_df["T3 S2 Label"]*100
    Tier_Chart_df["T3 S3 Label"] = Tier_Chart_df["T3 S3 Label"]*100

    Tier_Chart_df["Company S1 %"] = Tier_Chart_df["Company S1 %"]*100  
    Tier_Chart_df["Company S2 %"] = Tier_Chart_df["Company S2 %"]*100
    Tier_Chart_df["Company S3 %"] = Tier_Chart_df["Company S3 %"]*100
    Tier_Chart_df["T1 S1 %"] = Tier_Chart_df["T1 S1 %"]*100  
    Tier_Chart_df["T1 S2 %"] = Tier_Chart_df["T1 S2 %"]*100
    Tier_Chart_df["T1 S3 %"] = Tier_Chart_df["T1 S3 %"]*100    
    Tier_Chart_df["T2 S1 %"] = Tier_Chart_df["T2 S1 %"]*100  
    Tier_Chart_df["T2 S2 %"] = Tier_Chart_df["T2 S2 %"]*100
    Tier_Chart_df["T2 S3 %"] = Tier_Chart_df["T2 S3 %"]*100    
    Tier_Chart_df["T3 S1 %"] = Tier_Chart_df["T3 S1 %"]*100  
    Tier_Chart_df["T3 S2 %"] = Tier_Chart_df["T3 S2 %"]*100
    Tier_Chart_df["T3 S3 %"] = Tier_Chart_df["T3 S3 %"]*100
    
    Complete_Tier_Chart_df = {"Company S1 %" : [sum(Tier_Chart_df["Weighted Company S1 %"])*100], "Company S2 %" : [sum(Tier_Chart_df["Weighted Company S2 %"])*100], "Company S3 %" : [sum(Tier_Chart_df["Weighted Company S3 %"])*100], "T1 S1 %" : [sum(Tier_Chart_df["Weighted T1 S1 %"])*100], "T1 S2 %" : [sum(Tier_Chart_df["Weighted T1 S2 %"])*100], "T1 S3 %" : [sum(Tier_Chart_df["Weighted T1 S3 %"])*100],"T2 S1 %" : [sum(Tier_Chart_df["Weighted T2 S1 %"])*100],"T2 S2 %" : [sum(Tier_Chart_df["Weighted T2 S2 %"])*100],"T2 S3 %" : [sum(Tier_Chart_df["Weighted T2 S3 %"])*100],"T3 S1 %" : [sum(Tier_Chart_df["Weighted T3 S1 %"])*100],"T3 S2 %" : [sum(Tier_Chart_df["Weighted T3 S2 %"])*100],"T3 S3 %" : [sum(Tier_Chart_df["Weighted T3 S3 %"])*100], "Company S1 Label" : [sum(Tier_Chart_df["Weighted Company S1 Label"])*100], "Company S2 Label" : [sum(Tier_Chart_df["Weighted Company S2 Label"])*100], "Company S3 Label" : [sum(Tier_Chart_df["Weighted Company S3 Label"])*100],"T1 S1 Label" : [sum(Tier_Chart_df["Weighted T1 S1 Label"])*100], "T1 S2 Label" : [sum(Tier_Chart_df["Weighted T1 S2 Label"])*100], "T1 S3 Label" : [sum(Tier_Chart_df["Weighted T1 S3 Label"])*100],"T2 S1 Label" : [sum(Tier_Chart_df["Weighted T2 S1 Label"])*100],"T2 S2 Label" : [sum(Tier_Chart_df["Weighted T2 S2 Label"])*100],"T2 S3 Label" : [sum(Tier_Chart_df["Weighted T2 S3 Label"])*100],"T3 S1 Label" : [sum(Tier_Chart_df["Weighted T3 S1 Label"])*100],"T3 S2 Label" : [sum(Tier_Chart_df["Weighted T3 S2 Label"])*100],"T3 S3 Label" : [sum(Tier_Chart_df["Weighted T3 S3 Label"])*100],"T1 Count" : [sum(Tier_Chart_df["Weighted T1 Count"])],"T2 Count" : [sum(Tier_Chart_df["Weighted T2 Count"])],"T3 Count" : [sum(Tier_Chart_df["Weighted T3 Count"])],"T4 Count" : [sum(Tier_Chart_df["Weighted T4 Count"])]}
    
    Tier_Chart_df = Tier_Chart_df[['Commodities','Commodity Code','Adjusted Revenue','% of Revenue','Company S1 Label','Company S2 Label','Company S3 Label','T1 S1 Label','T1 S2 Label','T1 S3 Label','T2 S1 Label','T2 S2 Label','T2 S3 Label','T3 S1 Label','T3 S2 Label','T3 S3 Label','Company S1 %','Company S2 %','Company S3 %','T1 S1 %','T1 S2 %','T1 S3 %','T2 S1 %','T2 S2 %','T2 S3 %','T3 S1 %','T3 S2 %','T3 S3 %', 'T1 Count','T2 Count','T3 Count','T4 Count',]]
       
    Complete_Tier_Chart_df = pd.DataFrame(Complete_Tier_Chart_df)
    
def Tier_Commodities():
    global Tier_Commodity_Calculations_df, Tier1_S3_df, Tier2_S1_df, Tier2_S2_df, Tier2_S3_df, Tier3_S1_df, Tier3_S2_df, Tier3_S3_df, Tier4_S1_df, Tier4_S2_df, Tier4_S3_df
    Tier_Commodity_Calculations_df = Tier_Commodities_df.copy()[Tier_Commodities_df.index.isin(commodities_list)]
    Tier_Commodity_Calculations_df.reset_index(inplace=True)
    for i, row in Tier_Commodity_Calculations_df.iterrows():
        temp_revenue = json.loads(Get_Revenue_Summary_Value(str(Tier_Commodity_Calculations_df.loc[i,"Output_Commodity"])))
        Tier_Commodity_Calculations_df.loc[i,"Revenue"] = temp_revenue["Revenue"]
        Tier_Commodity_Calculations_df.loc[i,"S1 T1 * Rev"] = Tier_Commodity_Calculations_df.loc[i,"S1 T1"]*Tier_Commodity_Calculations_df.loc[i,"Revenue"]
        Tier_Commodity_Calculations_df.loc[i,"S2 T1 * Rev"] = Tier_Commodity_Calculations_df.loc[i,"S2 T1"]*Tier_Commodity_Calculations_df.loc[i,"Revenue"]
        Tier_Commodity_Calculations_df.loc[i,"S3 T1 * Rev"] = Tier_Commodity_Calculations_df.loc[i,"S3 T1"]*Tier_Commodity_Calculations_df.loc[i,"Revenue"]
        Tier_Commodity_Calculations_df.loc[i,"S1 T2 * Rev"] = Tier_Commodity_Calculations_df.loc[i,"S1 T2"]*Tier_Commodity_Calculations_df.loc[i,"Revenue"]
        Tier_Commodity_Calculations_df.loc[i,"S2 T2 * Rev"] = Tier_Commodity_Calculations_df.loc[i,"S2 T2"]*Tier_Commodity_Calculations_df.loc[i,"Revenue"]
        Tier_Commodity_Calculations_df.loc[i,"S3 T2 * Rev"] = Tier_Commodity_Calculations_df.loc[i,"S3 T2"]*Tier_Commodity_Calculations_df.loc[i,"Revenue"]
        Tier_Commodity_Calculations_df.loc[i,"S1 T3 * Rev"] = Tier_Commodity_Calculations_df.loc[i,"S1 T3"]*Tier_Commodity_Calculations_df.loc[i,"Revenue"]
        Tier_Commodity_Calculations_df.loc[i,"S2 T3 * Rev"] = Tier_Commodity_Calculations_df.loc[i,"S2 T3"]*Tier_Commodity_Calculations_df.loc[i,"Revenue"]
        Tier_Commodity_Calculations_df.loc[i,"S3 T3 * Rev"] = Tier_Commodity_Calculations_df.loc[i,"S3 T3"]*Tier_Commodity_Calculations_df.loc[i,"Revenue"]
   
    Tier_Commodity_Calculations_df.drop(["Output_Commodity"],axis = 1, inplace=True)
    Tier_Commodity_Calculations_df = pd.pivot_table(Tier_Commodity_Calculations_df,index=["Input_Commodity"], aggfunc = np.sum)
    Tier_Commodity_Calculations_df.reset_index(inplace=True)
    Scope3_Breakdown_df.set_index("IP_Comm", inplace=True)
    
    for j, row in Tier_Commodity_Calculations_df.iterrows():
        temp_Comm_Name = json.loads(Get_CodeMapping_Value(str(Tier_Commodity_Calculations_df.loc[j,"Input_Commodity"])))
        Tier_Commodity_Calculations_df.loc[j, "Description"] = temp_Comm_Name['Detail_Comm_Code_desc']
        Tier_Commodity_Calculations_df.loc[j, "S3 Company %"] = Scope3_Breakdown_df.loc[temp_Comm_Name['Detail_Comm_Code'],"% Scope 3 (DR)"]/100
        Tier_Commodity_Calculations_df.loc[j, "S1 T1 %"] = Tier_Commodity_Calculations_df.loc[j, "S1 T1 * Rev"]/sum(Tier_Commodity_Calculations_df["S1 T1 * Rev"])
        Tier_Commodity_Calculations_df.loc[j, "S2 T1 %"] = Tier_Commodity_Calculations_df.loc[j, "S2 T1 * Rev"]/sum(Tier_Commodity_Calculations_df["S2 T1 * Rev"])
        Tier_Commodity_Calculations_df.loc[j, "S3 T1 %"] = Tier_Commodity_Calculations_df.loc[j, "S3 T1 * Rev"]/sum(Tier_Commodity_Calculations_df["S3 T1 * Rev"])
        Tier_Commodity_Calculations_df.loc[j, "S1 T2 %"] = Tier_Commodity_Calculations_df.loc[j, "S1 T2 * Rev"]/sum(Tier_Commodity_Calculations_df["S1 T2 * Rev"])
        Tier_Commodity_Calculations_df.loc[j, "S2 T2 %"] = Tier_Commodity_Calculations_df.loc[j, "S2 T2 * Rev"]/sum(Tier_Commodity_Calculations_df["S2 T2 * Rev"])
        Tier_Commodity_Calculations_df.loc[j, "S3 T2 %"] = Tier_Commodity_Calculations_df.loc[j, "S3 T2 * Rev"]/sum(Tier_Commodity_Calculations_df["S3 T2 * Rev"])
        Tier_Commodity_Calculations_df.loc[j, "S1 T3 %"] = Tier_Commodity_Calculations_df.loc[j, "S1 T3 * Rev"]/sum(Tier_Commodity_Calculations_df["S1 T3 * Rev"])
        Tier_Commodity_Calculations_df.loc[j, "S2 T3 %"] = Tier_Commodity_Calculations_df.loc[j, "S2 T3 * Rev"]/sum(Tier_Commodity_Calculations_df["S2 T3 * Rev"])
        Tier_Commodity_Calculations_df.loc[j, "S3 T3 %"] = Tier_Commodity_Calculations_df.loc[j, "S3 T3 * Rev"]/sum(Tier_Commodity_Calculations_df["S3 T3 * Rev"])
        
    Tier_Commodity_Calculations_df = Tier_Commodity_Calculations_df[["Input_Commodity","Description","S3 Company %","S1 T1 %","S2 T1 %","S3 T1 %","S1 T2 %","S2 T2 %", "S3 T2 %","S1 T3 %","S2 T3 %", "S3 T3 %"]]

    
## To save the outputs of the analysis in a fixed template - which serves as input to tableau.
def Analysis_Tableau():
    print ("Preparing results...")
    global dashboard_userinput_df, Analysis_Tableau_df, Purchased_Commodities_list, Model_OP_411_df, Total_Company_Carbon_emissions, Total_Company_Scope1 , Total_Company_Scope2, Total_Company_Scope3, Final_results_df
    global Percentage_Company_Scope1,Percentage_Company_Scope2, Percentage_Company_Scope3
    temp_df = dashboard_userinput_df[dashboard_userinput_df['Commodity Code']!="-"].copy()
    Analysis_Tableau_df = temp_df.loc[: , ['Commodity Code','Producer/Purchaser','Revenue ($)']].copy()
    Analysis_Tableau_df.reset_index(inplace=True)
    # Adding cols for analysis_results
    for j, row in Analysis_Tableau_df.iterrows():
        ## Year_adj
        temp_Year_adj = json.loads(Get_Year_adj_Value(str(Analysis_Tableau_df.loc[j,"Commodity Code"]),Year))
        Analysis_Tableau_df.loc[j,"Year Price adjustment"] = temp_Year_adj["Year_Adj"]
        
        ## Producer/Purchaser adj, only if user enters "Purchased" for a commodity code else the adj value remains 1.
        if j in Purchased_Commodities_list:
            temp_Pr_adj = json.loads(Get_Prod_Purchaser_Value(str(Analysis_Tableau_df.loc[j,"Commodity Code"]),Year))
            Analysis_Tableau_df.loc[j,"Purchase Price adjustment"] = temp_Pr_adj["Pro_Pur_adj"]
        else:
            Analysis_Tableau_df.loc[j,"Purchase Price adjustment"] = 1
            
        ## Total Emission Factor value, % Scope 1, % Scope 2, % Scope 3
        temp_OP_Value = json.loads(Get_ModelOP_Value(str(Analysis_Tableau_df.loc[j,"Commodity Code"])))
        Analysis_Tableau_df.loc[j,"Total Emission Factor"] = temp_OP_Value["TEF_Val"]
        Analysis_Tableau_df.loc[j,"Model Percentage Scope 1"] = round(temp_OP_Value["Scope_1"],6)
        Analysis_Tableau_df.loc[j,"Model Percentage Scope 2"] = round(temp_OP_Value["Scope_2"],6)
        Analysis_Tableau_df.loc[j,"Model Percentage Scope 3"] = round(temp_OP_Value["Scope_3"],6)

        ## Total Emissions
        Analysis_Tableau_df.loc[j,"Total Emissions (Co2e kg)"] = Analysis_Tableau_df.loc[j,"Revenue ($)"] * Analysis_Tableau_df.loc[j,"Year Price adjustment"] * Analysis_Tableau_df.loc[j,"Purchase Price adjustment"] * Analysis_Tableau_df.loc[j,"Total Emission Factor"]
        
        ## Scope 1/ Scope 2/ Scope 3 Emissions
        Analysis_Tableau_df.loc[j,"Scope 1 Emissions (Co2e kg)"] =  Analysis_Tableau_df.loc[j,"Total Emissions (Co2e kg)"] * Analysis_Tableau_df.loc[j,"Model Percentage Scope 1"]/100
        Analysis_Tableau_df.loc[j,"Scope 2 Emissions (Co2e kg)"] =  Analysis_Tableau_df.loc[j,"Total Emissions (Co2e kg)"] * Analysis_Tableau_df.loc[j,"Model Percentage Scope 2"]/100
        Analysis_Tableau_df.loc[j,"Scope 3 Emissions (Co2e kg)"] =  Analysis_Tableau_df.loc[j,"Total Emissions (Co2e kg)"] * Analysis_Tableau_df.loc[j,"Model Percentage Scope 3"]/100
    ## Final Results for each input commodity
    Total_Company_Carbon_emissions = sum(Analysis_Tableau_df["Total Emissions (Co2e kg)"])
    Total_Company_Scope1 = sum(Analysis_Tableau_df["Scope 1 Emissions (Co2e kg)"])
    Total_Company_Scope2 = sum(Analysis_Tableau_df["Scope 2 Emissions (Co2e kg)"])
    Total_Company_Scope3 = sum(Analysis_Tableau_df["Scope 3 Emissions (Co2e kg)"])
    Percentage_Company_Scope1 = (Total_Company_Scope1)*100/(Total_Company_Carbon_emissions)
    Percentage_Company_Scope2 = (Total_Company_Scope2)*100/(Total_Company_Carbon_emissions)
    Percentage_Company_Scope3 = (Total_Company_Scope3)*100/(Total_Company_Carbon_emissions)
    Final_Results_data = { "Total Carbon Emissions" : [Total_Company_Carbon_emissions],
                           "Total_Scope1_Emissions" : [Total_Company_Scope1],
                           "Total_Scope2_Emissions" : [Total_Company_Scope2],
                           "Total_Scope3_Emissions" : [Total_Company_Scope3],
                           "Percentage_Scope1"      : [(Total_Company_Scope1*100)/Total_Company_Carbon_emissions],
                           "Percentage_Scope2"      : [(Total_Company_Scope2*100)/Total_Company_Carbon_emissions],
                           "Percentage_Scope3"      : [(Total_Company_Scope3*100)/Total_Company_Carbon_emissions]}
    Final_results_df = pd.DataFrame(Final_Results_data)
    
    
    for k, row in Analysis_Tableau_df.iterrows():
        Analysis_Tableau_df.loc[k,"Percentage of Total emissions"] = Analysis_Tableau_df.loc[k,"Total Emissions (Co2e kg)"]*100/Total_Company_Carbon_emissions
    # Arranging the Columns
    Analysis_Tableau_df =  Analysis_Tableau_df[['Commodities', 'Commodity Code', 'Producer/Purchaser', 'Revenue ($)','Year Price adjustment', 'Purchase Price adjustment','Total Emission Factor', 'Total Emissions (Co2e kg)','Percentage of Total emissions', 'Scope 1 Emissions (Co2e kg)', 'Scope 2 Emissions (Co2e kg)', 'Scope 3 Emissions (Co2e kg)', 'Model Percentage Scope 1','Model Percentage Scope 2', 'Model Percentage Scope 3']]
    
## To save the outputs of the analysis in a fixed readable format.
def Analysis2():
    global dashboard_userinput_df, Analysis_Tableau_df,Analysis_2_df,  Purchased_Commodities_list, Model_OP_411_df, Total_Company_Carbon_emissions, Total_Company_Scope1 , Total_Company_Scope2, Total_Company_Scope3
    Analysis_2_df = Analysis_Tableau_df.loc[: , ['Commodities','Commodity Code','Producer/Purchaser','Revenue ($)','Scope 1 Emissions (Co2e kg)', 'Scope 2 Emissions (Co2e kg)', 'Scope 3 Emissions (Co2e kg)', 'Total Emissions (Co2e kg)']].copy()
    for t, row in Analysis_Tableau_df.iterrows():
        temp_Comm_Name_Value = json.loads(Get_CodeMapping_Value(str(Analysis_2_df.loc[t,"Commodity Code"])))
        Analysis_2_df.loc[t,"Commodity Name"] = temp_Comm_Name_Value['Detail_Comm_Code_desc']
        Analysis_2_df.loc[t,"Percentage of Scope 1"] = Analysis_2_df.loc[t,"Scope 1 Emissions (Co2e kg)"]*100/sum(Analysis_2_df['Scope 1 Emissions (Co2e kg)'])
        Analysis_2_df.loc[t,"Percentage of Scope 2"] = Analysis_2_df.loc[t,"Scope 2 Emissions (Co2e kg)"]*100/sum(Analysis_2_df['Scope 2 Emissions (Co2e kg)'])
        Analysis_2_df.loc[t,"Percentage of Scope 3"] = Analysis_2_df.loc[t,"Scope 3 Emissions (Co2e kg)"]*100/sum(Analysis_2_df['Scope 3 Emissions (Co2e kg)'])
        Analysis_2_df.loc[t,"Percentage of Total"] =  Analysis_2_df.loc[t,"Total Emissions (Co2e kg)"]*100/sum(Analysis_2_df["Total Emissions (Co2e kg)"])
    # Sorting the columns
    Analysis_2_df = Analysis_2_df[['Commodities', 'Commodity Name', 'Commodity Code', 'Producer/Purchaser', 'Revenue ($)', 'Scope 1 Emissions (Co2e kg)', 'Scope 2 Emissions (Co2e kg)', 'Scope 3 Emissions (Co2e kg)', 'Total Emissions (Co2e kg)', 'Percentage of Scope 1', 'Percentage of Scope 2','Percentage of Scope 3', 'Percentage of Total']]


def SaveJSONfiles():
    print ("Saving json results to OUTPUTS folder")
    global Analysis_Tableau_df, Analysis_Tableau_df, Scope3_Breakdown_df, Scope3_Breakdown_DR_Sorted, companies_seg_df, Scope3_Breakdown_Summary_df_DR_Sorted, Scope3_Breakdown_Consolidated_df_DR_Sorted
    
    jsonResponse = {}

    orient_val = 'records'

    jsonResponse['CarbonEmissions_Analysis'] = json.loads(Analysis_Tableau_df.to_json(orient=orient_val, indent=4))
    jsonResponse['Scope3_Breakdown_TR_Sorted'] = json.loads(Scope3_Breakdown_df.to_json(orient=orient_val, indent=4))
    jsonResponse['Scope3_Breakdown_DR_Sorted'] = json.loads(Scope3_Breakdown_df_DR_Sorted.to_json(orient=orient_val, indent=4))
    jsonResponse['Scope3_Breakdown_Summ_TR_Sorted'] = json.loads(Scope3_Breakdown_Summary_df.to_json(orient=orient_val, indent=4))
    jsonResponse['Scope3_Breakdown_Summ_DR_Sorted'] = json.loads(Scope3_Breakdown_Summary_df_DR_Sorted.to_json(orient=orient_val, indent=4))
    jsonResponse['Scope3_Breakdown_Cons_TR_Sorted'] = json.loads(Scope3_Breakdown_Consolidated_df.to_json(orient=orient_val, indent=4))
    jsonResponse['Scope3_Breakdown_Cons_DR_Sorted'] = json.loads(Scope3_Breakdown_Consolidated_df_DR_Sorted.to_json(orient=orient_val, indent=4))
    jsonResponse['S1S2S3_Results'] = json.loads(Final_results_df.to_json(orient=orient_val, indent=4))
    jsonResponse['Tier_Chart'] = json.loads(Tier_Chart_df.to_json(orient=orient_val, indent=4))
    jsonResponse['Complete_Tier_Chart'] = json.loads(Complete_Tier_Chart_df.to_json(orient=orient_val, indent=4))
    jsonResponse['Tier_Commodities'] = json.loads(Tier_Commodity_Calculations_df.to_json(orient=orient_val, indent=4))

    return jsonResponse    
    
   #***************** SECTION 4: MAIN *****************
   #######################################
   # FileReadin
   # Purpose           : Calling out the functions written above sequentially to perform the analysis.
   ####################################### 

Read_Static_Excel_Files()

print ("Done!!")
print ("--------------------")