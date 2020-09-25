import pandas as pd
import numpy as np

# original dataframe
data=pd.read_csv("data_sources/catalog_product_ors_full_data.csv")

# dataframe with new columns and values
extra_coldata=pd.read_excel("data_sources/Trained_entities.xlsx")

class Appender:
    def __init__(self,dataframe):
        self.dataframe=dataframe
        print(self.dataframe.head())
        print(self.dataframe.shape)
    def append_extra_colvals(self,extra_colvals,cols_column,values_column,delimiter=","):
        """
            This function takes a dataframe with a column with extra attributes 
            and a column with list of samples of each extra attribute
            parameters
            -----------
            param extra_colvals: dataframe with extra attributes and their values
            cols_column: column containing extra attributes
            values_column: column containing attribute values 
        """
        self.extra_data=extra_colvals
        self.extra_cols=self.extra_data[cols_column]
        self.extra_colvals=self.extra_data[values_column].str.split(delimiter)
    def get_new_dataframe(self):
        """
            This function return new dataframe after appending new attribute columns to original dataframe
        """
        new_df=self.dataframe.copy()
        print(f"Adding {len(self.extra_cols)} columns")
        print("-"*50)
        for i in range(len(self.extra_cols)):
            if self.extra_cols[i] not in new_df.columns:
                print(f"Adding {self.extra_cols[i]} data")

                # resizing the values
                new_df[self.extra_cols[i]]=np.resize(self.extra_colvals[i],len(new_df))
            else:
                print(f"{self.extra_cols[i]} already present in dataframe")
        self.new_df=new_df
        return self.new_df
    def save(self,filename,format):
        """
            This function saves the new dataframe generated

            parameters
            ----------
            filename : filename to save the new dataframe
            format : supported formats excel or csv
        """
        if format=="excel":
            self.new_df.to_excel(filename,index=False)
            print(f"{filename} saved...")
        elif format=="csv":
            self.new_df.to_csv(filename,index=False)
            print(f"{filename} saved...")
        else:
            raise AttributeError("pass valid format, supported formats:excel or csv")

app=Appender(data)
app.append_extra_colvals(extra_coldata,"ES mapper","Sample Values")
new_df=app.get_new_dataframe()
print(f"original dataframe shape:{data.shape}")
print(f"new datafram shape: {new_df.shape}")
app.save("data_sources/ors_new.xlsx","excel")


