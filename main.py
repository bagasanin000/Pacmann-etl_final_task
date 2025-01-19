import luigi
from etl_luigi import ExtractMarketingData, ExtractNewsData, ExtractSalesData, TransformMarketingData, TransformSalesData, TransformNewsData, ValidateData, LoadData 
from helper.dir_cleaner import clean_output

if __name__ == "__main__":
    luigi.build([ExtractMarketingData(),
                ExtractSalesData(),
                ExtractNewsData(),
                ValidateData(),
                TransformMarketingData(),
                TransformSalesData(),
                TransformNewsData(),
                LoadData()])
    clean_output()