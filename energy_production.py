from datetime import datetime
import eurostat

# Net electricity generation by type of fuel - monthly data (nrg_cb_pem)	
# Metadata: https://ec.europa.eu/eurostat/cache/metadata/en/nrg_quant_esms.htm

try:
    print("Try to fetch new Eurostat data for monthly net energy generation")
    raw_d = (eurostat.get_data_df("nrg_cb_pem")
        .rename(columns={"geo\\TIME_PERIOD":"geo"})
    )
except:
    print("Failed to fetch data from Eurostat.")
else:
    out = (
        raw_d.query('freq == "M"') # for extra safety - only monthly as expected
        .melt(id_vars=["freq", "siec", "unit", "geo"])
        .dropna(subset="value")
    ) 

datum = datetime.now().strftime("%Y-%m-%d")
out_name = f"datasets/EU net monthly Energy Generation {datum}.parquet"

try:
    out.to_parquet(out_name, engine="fastparquet")
    print(f"Saved new data for Eurostat nrg_cb_pem to {out_name}")
except:
    print("Failed to save data to disk.")
finally:
    print("Finished.")