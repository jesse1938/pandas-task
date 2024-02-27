import json
import requests
from loguru import logger
import pandas as pd

from tabulate import tabulate

# generates the asset, exposure and aum reports
# all  the data is derived from the account summary endpoint on heroku
# the data is then parsed and formatted into tables and sent to slack
# the aum is calculated by summing the equity_usd for each asset
# the exposure is calculated by summing the deltaUsd for each coin

# TODO - report exceptions elsewhere




def format_equity_usd(value):
    try:
    # Attempt to convert value to float if it's not already a numeric type
        numeric_value = float(value)
    except ValueError:
        # If conversion fails, return the value as is or handle the error as appropriate
        return value
    if abs(value) > 1000000:
        #return number of millions rounded to 1 decimal
        return f"${value/1000000:.1f} M"
    elif value < 1000:
        return f"${int(value)} -"
    else:
        return f"${int(value/1000)} K"
    

def find_future_positions(json_obj):
    '''takes a json object from account summary endpoint on heroku, parses out the future positions
      and returns a list of dictionaries with the position details'''
    future_positions = []
    venues = json_obj['result']['venues']
    for _venue in venues:
        for position in _venue['futuresPositions']:
            sybmol = position['symbol']
            sizeUsd = position['sizeUsd']
            side = position['side']
            pnlUsd = position['pnlUsd']
            venue = position['venue']
            coin = position['coin']
            liq_price = position['estimatedLiquidationPrice']
            deltaUsd = position['deltaUsd']
            mark_price = position['markPx']
            fut_dict = {'symbol': sybmol, 'sizeUsd': sizeUsd, 'side': side, 'pnlUsd': pnlUsd, 'venue': venue, 'coin': coin, 'liq_price': liq_price, 'deltaUsd': deltaUsd, 'mark_price': mark_price}
            future_positions.append(fut_dict)
            print(fut_dict)

    return future_positions

def create_future_position_df(future_positions):
    df = pd.DataFrame(future_positions)
    return df

def generate_liquidation_table(df):
    def calculate_percent_to_liq(row):
        # Ensure 'liq_price' and 'mark_price' are floats
        liq_price = float(row['liq_price'])
        mark_price = float(row['mark_price'])

        # Initialize 'percent_to_liq' to 0 if 'liq_price' is 0 to avoid division by zero
        if liq_price == 0.0:
            row['percent_to_liq'] = 100
        # Calculate 'percent_to_liq' for 'buy' side
        elif row['side'] == 'buy':
            row['percent_to_liq'] = (mark_price - liq_price) / mark_price * 100
        # Calculate 'percent_to_liq' for 'sell' side
        elif row['side'] == 'sell':
            row['percent_to_liq'] = (liq_price - mark_price) / mark_price * 100

        return row

    # Apply the function across each row
    df = df.apply(calculate_percent_to_liq, axis=1)

    # sort the df by percent to liq in descending order
    df = df.sort_values(by='percent_to_liq', ascending=False)

    # strip out the ones that are more than 100% to liq
    df = df[df['percent_to_liq'] < 100]

    # only show integers on percent to liq
    df['percent_to_liq'] = df['percent_to_liq'].astype(int)

    # only show rows within liqudaition threshold
    df = df[abs(df['percent_to_liq']) < 25]

    # format the sizeUSD column
    df['sizeUsd'] = df['sizeUsd'].apply(format_equity_usd)

    columns = ['symbol','venue','side','sizeUsd','mark_price','liq_price','percent_to_liq']
    df = df[columns]
    table= tabulate(df, headers='keys', tablefmt='psql', showindex=False)
    return table

def aggregate_exposures_by_coin(df):
    aggregated_df = df.groupby(['coin'])['deltaUsd'].sum().reset_index()
    aggregated_df = aggregated_df.rename(columns={'deltaUsd': 'derivExposureUsd'})

    # sort aggregated data descenting by 'totalSizeUsd'
    aggregated_df = aggregated_df.sort_values(by='derivExposureUsd', ascending=True)

    return aggregated_df

def generate_deriv_exposure_table(df):
    
    df['derivExposureUsd'] = df['derivExposureUsd']

    table = tabulate(df, headers='keys', tablefmt='pipe', showindex=False)
    print(table)

def generate_aggregated_exposure_table(deriv_df, spot_df):
    # Rename columns in deriv_df and spot_df for clarity and consistency
    print(deriv_df.columns)
    print(spot_df.columns)

    deriv_df = deriv_df.rename(columns={'derivExposureUsd': 'deriv_total'})
    spot_df = spot_df.rename(columns={'equity_usd': 'spot_total'})
    spot_df = spot_df.rename(columns={'asset': 'coin'})
    
    # Ensure 'coin' column exists and is set as index for merging
    deriv_df.set_index('coin', inplace=True)
    spot_df.set_index('coin', inplace=True)

    
    # Merge the two dataframes on 'coin' index, using outer join to include all coins
    combined_df = pd.merge(spot_df[['spot_total']], deriv_df[['deriv_total']], left_index=True, right_index=True, how='outer')
    
    # Fill NaN values with 0 or a suitable placeholder
    combined_df.fillna(0, inplace=True)
    
    # Reset index to bring 'coin' back as a column
    combined_df.reset_index(inplace=True)
    
    # compute the total column
    combined_df['total'] = combined_df['spot_total'] + combined_df['deriv_total']


    # get the aum
    aum_total = combined_df['spot_total'].sum()

    # strip out the stable coins 
    combined_df = combined_df[~combined_df['coin'].str.contains('USD|USDT|USDC')]

    # sum the total of each column
    total_spot = combined_df['spot_total'].sum()
    total_deriv = combined_df['deriv_total'].sum()
    total_combined = combined_df['total'].sum()
    
    # compute the exposure% column
    combined_df['exposure %'] = combined_df['total'] / total_combined * 100

    # strip out coins with < 1000 in exposure
    combined_df = combined_df[combined_df['exposure %'].abs() > 1]
       
    #sort the df by combined total
    combined_df = combined_df.sort_values(by='total', ascending=False)

    # Create a copy of the dataframe to apply formatting
    formatted_df = combined_df.copy()

    # Apply formatting to 'Spot_total' and 'deriv_total' columns if necessary
    formatted_df['spot_total'] = formatted_df['spot_total'].apply(format_equity_usd)
    formatted_df['deriv_total'] = formatted_df['deriv_total'].apply(format_equity_usd)
    formatted_df['total'] = formatted_df['total'].apply(format_equity_usd)
    
    # Apply formatting to 'exposure %' column, rounded to nearest percent
    formatted_df['exposure %'] = formatted_df['exposure %'].apply(lambda x: f"{round(x)}")


    # Generate and print the table
    table = tabulate(formatted_df, headers='keys', tablefmt='pipe', showindex=False)
    
    print(table)
    combined_data = {'table': table, 'total_spot': total_spot, 'total_deriv': total_deriv, 'total_combined': total_combined, 'aum': aum_total}
    
    return combined_data

def parse_cefi_equities_from_acct_summary(json_obj):
    venues = []
    venues = json_obj['result']['venues']
    equities = []
    for venue in venues:
        venue_acct = venue['venueAccount']
        venue_name = venue['venue']
        for balance in venue['balances']:
            asset = balance['asset']
            equity = balance['equity']
            ref_px = balance['refPx']   
            equity_usd = balance['equityUsd']
            print(f'{venue_acct}: Asset: {asset}, Equity: {equity}, RefPx: {ref_px}, EquityUsd: {equity_usd}')
            
            # make a dictionary
            equity_dict = {'venue_acct': venue_acct, 'asset': asset, 'equity': equity, 'ref_px': ref_px, 'equity_usd': equity_usd, 'venue_name': venue_name}     
            equities.append(equity_dict)
    return equities
    #print(venues)

def parse_defi_equities_from_account_summary(json_obj):
    venues = []
    venues = json_obj['result']['venues']
    equities = []
    for venue in venues: 
        if venue['venueType'] == 'DeFi':
            venue_acct = venue['venueAccount']
            venue_name = venue['venue']
            for balance in venue['walletInventory']:
                asset = balance['asset']
                equity = balance['equity']
                ref_px = balance['refPx']   
                equity_usd = balance['equityUsd']
                print(f'{venue_acct}: Asset: {asset}, Equity: {equity}, RefPx: {ref_px}, EquityUsd: {equity_usd}')
                
                # make a dictionary
                equity_dict = {'venue_acct': venue_acct, 'asset': asset, 'equity': equity, 'ref_px': ref_px, 'equity_usd': equity_usd, 'venue_name': venue_name }     
                equities.append(equity_dict)
                
    return equities
    
def calculate_total_equity(equities):
    total_equity = 0
    for equity in equities:
        total_equity += equity['equity_usd']
        
    print(f'Total Equity: {total_equity}')
    return total_equity

def calculate_equity_by_venue(equities):
    venue_equities = {}
    for equity in equities:
        venue_name = equity['venue_name']
        equity_usd = equity['equity_usd']
        if venue_name in venue_equities:
            venue_equities[venue_name] += equity_usd
        else:
            venue_equities[venue_name] = equity_usd
    print(venue_equities)
    return venue_equities

def calculate_equity_by_asset(equities):
    asset_equities = {}
    for equity in equities:
        asset = equity['asset']
        equity_value = equity['equity']  # Assuming this is a numeric value you want to aggregate
        equity_usd = equity['equity_usd']
        if asset not in asset_equities:
            asset_equities[asset] = {'equity': 0, 'equity_usd': 0}
        asset_equities[asset]['equity'] += equity_value
        asset_equities[asset]['equity_usd'] += equity_usd
    
    # Convert the aggregated equity values into a list of dictionaries
    asset_equities_list = [{'asset': asset, 'equity': values['equity'], 'equity_usd': values['equity_usd']} for asset, values in asset_equities.items()]
    
    return asset_equities_list
    

def generate_asset_table(asset_equities):
    df = pd.DataFrame(asset_equities)
    df['equity_usd'] = df['equity_usd'].replace('[\$,]', '', regex=True).astype(float)

    # Aggregate data by 'asset'
    aggregated_data = df.groupby('asset').agg({'equity': 'sum', 'equity_usd': 'sum'}).reset_index()
    
    # sort aggregated data descenting by 'equity_usd'
    aggregated_data_df = aggregated_data.sort_values(by='equity_usd', ascending=False)

    # Custom formatting for 'equity' to display <1 for values less than 1
    def format_equity(value):
        if value < 1:
            return "< 1"
        else:
            return f"{value:.2f}"

    # Apply formatting only for the table string
    formatted_aggregated_data = aggregated_data.copy()
    formatted_aggregated_data['equity'] = formatted_aggregated_data['equity'].apply(format_equity)
    
    formatted_aggregated_data['equity_usd'] = formatted_aggregated_data['equity_usd'].apply(format_equity_usd)

    # Generate table string with formatted data
    table_string = tabulate(formatted_aggregated_data, headers='keys', tablefmt='pipe', showindex=False)
    
    print(table_string)
    
    return table_string, aggregated_data_df


def calculate_total_aum(aums):
    total_aum = 0
    for aum in aums:
        total_aum += aum['total_equity']
    return total_aum


def test_with_file():
    aums = []
    with open('aum_test.json') as f:
        acct_summary = json.load(f)

    # parse cefi and defi equities
    cefi_equities = parse_cefi_equities_from_acct_summary(acct_summary)
    defi_equities = parse_defi_equities_from_account_summary(acct_summary)
    equities = cefi_equities + defi_equities
    total_equity = calculate_total_equity(equities)
    
    table, aggregated_data_df = generate_asset_table(equities)

    #generate futures positions df
    futures_summary = find_future_positions(acct_summary)
    df = create_future_position_df(futures_summary)
    aggregated_df = aggregate_exposures_by_coin(df)
    table = generate_deriv_exposure_table(aggregated_df)

    # generate aggregated exposure table, send to slack
    combined_data = generate_aggregated_exposure_table(aggregated_df, aggregated_data_df)
    combined_exposure_table = combined_data['table']
    total_spot = combined_data['total_spot']
    total_deriv = combined_data['total_deriv']
    total_combined = combined_data['total_combined']
            
    #send_slack_assets(table, group_name, TARGET_CHANNEL, ts)
    # generate liquidation table, send to slack
    liquidation_table = generate_liquidation_table(df)

    
    
    
    
if __name__ == '__main__':
    test_with_file()
    #main()
    #test_with_pulled_data()
 
    #test_with_file()
    #equities = parse_json(json_obj)
    #calculate_total_equity(equities)
    