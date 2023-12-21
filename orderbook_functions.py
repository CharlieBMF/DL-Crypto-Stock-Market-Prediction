def fill_bids_value_to_range(bid, orderbook_df, orderbook_percent):
    if orderbook_percent < 0.001:
        orderbook_df.at[len(orderbook_df) - 1, 'b0-0.001'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                 'b0-0.001'] + float(bid[1])
        orderbook_df.at[len(orderbook_df) - 1, 'b0-0.002'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                 'b0-0.002'] + float(bid[1])
        orderbook_df.at[len(orderbook_df) - 1, 'b0-0.005'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                 'b0-0.005'] + float(bid[1])
    elif 0.001 <= orderbook_percent < 0.002:
        orderbook_df.at[len(orderbook_df) - 1, 'b0.001-0.002'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                     'b0.001-0.002'] + float(bid[1])
        orderbook_df.at[len(orderbook_df) - 1, 'b0-0.002'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                 'b0-0.002'] + float(bid[1])
        orderbook_df.at[len(orderbook_df) - 1, 'b0-0.005'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                 'b0-0.005'] + float(bid[1])
    elif 0.002 <= orderbook_percent < 0.003:
        orderbook_df.at[len(orderbook_df) - 1, 'b0.002-0.003'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                     'b0.002-0.003'] + float(bid[1])
        orderbook_df.at[len(orderbook_df) - 1, 'b0.002-0.004'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                     'b0.002-0.004'] + float(bid[1])
        orderbook_df.at[len(orderbook_df) - 1, 'b0-0.005'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                 'b0-0.005'] + float(bid[1])
    elif 0.003 <= orderbook_percent < 0.004:
        orderbook_df.at[len(orderbook_df) - 1, 'b0.003-0.004'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                     'b0.003-0.004'] + float(bid[1])
        orderbook_df.at[len(orderbook_df) - 1, 'b0.002-0.004'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                     'b0.002-0.004'] + float(bid[1])
        orderbook_df.at[len(orderbook_df) - 1, 'b0-0.005'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                 'b0-0.005'] + float(bid[1])
    elif 0.004 <= orderbook_percent < 0.005:
        orderbook_df.at[len(orderbook_df) - 1, 'b0.004-0.005'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                     'b0.001-0.002'] + float(bid[1])
        orderbook_df.at[len(orderbook_df) - 1, 'b0.004-0.006'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                     'b0.004-0.006'] + float(bid[1])
        orderbook_df.at[len(orderbook_df) - 1, 'b0-0.005'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                 'b0-0.005'] + float(bid[1])
    elif 0.005 <= orderbook_percent < 0.006:
        orderbook_df.at[len(orderbook_df) - 1, 'b0.005-0.006'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                     'b0.001-0.002'] + float(bid[1])
        orderbook_df.at[len(orderbook_df) - 1, 'b0.004-0.006'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                     'b0.004-0.006'] + float(bid[1])
        orderbook_df.at[len(orderbook_df) - 1, 'b0.005-0.01'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                    'b0.005-0.01'] + float(bid[1])
    elif 0.006 <= orderbook_percent < 0.007:
        orderbook_df.at[len(orderbook_df) - 1, 'b0.006-0.007'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                     'b0.006-0.007'] + float(bid[1])
        orderbook_df.at[len(orderbook_df) - 1, 'b0.006-0.008'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                     'b0.006-0.008'] + float(bid[1])
        orderbook_df.at[len(orderbook_df) - 1, 'b0.005-0.01'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                    'b0.005-0.01'] + float(bid[1])
    elif 0.007 <= orderbook_percent < 0.008:
        orderbook_df.at[len(orderbook_df) - 1, 'b0.007-0.008'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                     'b0.007-0.008'] + float(bid[1])
        orderbook_df.at[len(orderbook_df) - 1, 'b0.006-0.008'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                     'b0.006-0.008'] + float(bid[1])
        orderbook_df.at[len(orderbook_df) - 1, 'b0.005-0.01'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                    'b0.005-0.01'] + float(bid[1])
    elif 0.008 <= orderbook_percent < 0.009:
        orderbook_df.at[len(orderbook_df) - 1, 'b0.008-0.009'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                     'b0.008-0.009'] + float(bid[1])
        orderbook_df.at[len(orderbook_df) - 1, 'b0.008-0.01'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                    'b0.008-0.01'] + float(bid[1])
        orderbook_df.at[len(orderbook_df) - 1, 'b0.005-0.01'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                    'b0.005-0.01'] + float(bid[1])
    elif 0.009 <= orderbook_percent < 0.01:
        orderbook_df.at[len(orderbook_df) - 1, 'b0.009-0.01'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                    'b0.009-0.01'] + float(bid[1])
        orderbook_df.at[len(orderbook_df) - 1, 'b0.008-0.01'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                    'b0.008-0.01'] + float(bid[1])
        orderbook_df.at[len(orderbook_df) - 1, 'b0.005-0.01'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                    'b0.005-0.01'] + float(bid[1])
    elif 0.01 <= orderbook_percent < 0.015:
        orderbook_df.at[len(orderbook_df) - 1, 'b0.01-0.015'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                    'b0.01-0.015'] + float(bid[1])
        orderbook_df.at[len(orderbook_df) - 1, 'b0.01-0.02'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                   'b0.01-0.02'] + float(bid[1])
        orderbook_df.at[len(orderbook_df) - 1, 'b0.01-0.05'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                   'b0.01-0.05'] + float(bid[1])
    elif 0.015 <= orderbook_percent < 0.02:
        orderbook_df.at[len(orderbook_df) - 1, 'b0.015-0.02'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                    'b0.015-0.02'] + float(bid[1])
        orderbook_df.at[len(orderbook_df) - 1, 'b0.01-0.02'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                   'b0.01-0.02'] + float(bid[1])
        orderbook_df.at[len(orderbook_df) - 1, 'b0.01-0.05'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                   'b0.01-0.05'] + float(bid[1])
    elif 0.02 <= orderbook_percent < 0.025:
        orderbook_df.at[len(orderbook_df) - 1, 'b0.02-0.025'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                    'b0.02-0.025'] + float(bid[1])
        orderbook_df.at[len(orderbook_df) - 1, 'b0.02-0.03'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                   'b0.02-0.03'] + float(bid[1])
        orderbook_df.at[len(orderbook_df) - 1, 'b0.01-0.05'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                   'b0.01-0.05'] + float(bid[1])
    elif 0.025 <= orderbook_percent < 0.03:
        orderbook_df.at[len(orderbook_df) - 1, 'b0.025-0.03'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                    'b0.025-0.03'] + float(bid[1])
        orderbook_df.at[len(orderbook_df) - 1, 'b0.02-0.03'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                   'b0.02-0.03'] + float(bid[1])
        orderbook_df.at[len(orderbook_df) - 1, 'b0.01-0.05'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                   'b0.01-0.05'] + float(bid[1])
    elif 0.03 <= orderbook_percent < 0.035:
        orderbook_df.at[len(orderbook_df) - 1, 'b0.03-0.035'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                    'b0.03-0.035'] + float(bid[1])
        orderbook_df.at[len(orderbook_df) - 1, 'b0.03-0.04'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                   'b0.03-0.04'] + float(bid[1])
        orderbook_df.at[len(orderbook_df) - 1, 'b0.01-0.05'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                   'b0.01-0.05'] + float(bid[1])
    elif 0.035 <= orderbook_percent < 0.04:
        orderbook_df.at[len(orderbook_df) - 1, 'b0.035-0.04'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                    'b0.035-0.04'] + float(bid[1])
        orderbook_df.at[len(orderbook_df) - 1, 'b0.03-0.04'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                   'b0.03-0.04'] + float(bid[1])
        orderbook_df.at[len(orderbook_df) - 1, 'b0.01-0.05'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                   'b0.01-0.05'] + float(bid[1])
    elif 0.04 <= orderbook_percent < 0.045:
        orderbook_df.at[len(orderbook_df) - 1, 'b0.04-0.045'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                    'b0.04-0.045'] + float(bid[1])
        orderbook_df.at[len(orderbook_df) - 1, 'b0.04-0.05'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                   'b0.04-0.05'] + float(bid[1])
        orderbook_df.at[len(orderbook_df) - 1, 'b0.01-0.05'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                   'b0.01-0.05'] + float(bid[1])
    elif 0.045 <= orderbook_percent < 0.05:
        orderbook_df.at[len(orderbook_df) - 1, 'b0.045-0.05'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                    'b0.045-0.05'] + float(bid[1])
        orderbook_df.at[len(orderbook_df) - 1, 'b0.04-0.05'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                   'b0.04-0.05'] + float(bid[1])
        orderbook_df.at[len(orderbook_df) - 1, 'b0.01-0.05'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                   'b0.01-0.05'] + float(bid[1])
    elif 0.05 <= orderbook_percent < 0.055:
        orderbook_df.at[len(orderbook_df) - 1, 'b0.05-0.055'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                    'b0.05-0.055'] + float(bid[1])
        orderbook_df.at[len(orderbook_df) - 1, 'b0.05-0.06'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                   'b0.05-0.06'] + float(bid[1])
        orderbook_df.at[len(orderbook_df) - 1, 'b0.05-0.1'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                  'b0.05-0.1'] + float(bid[1])
    elif 0.055 <= orderbook_percent < 0.06:
        orderbook_df.at[len(orderbook_df) - 1, 'b0.055-0.06'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                    'b0.055-0.06'] + float(bid[1])
        orderbook_df.at[len(orderbook_df) - 1, 'b0.05-0.06'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                   'b0.05-0.06'] + float(bid[1])
        orderbook_df.at[len(orderbook_df) - 1, 'b0.05-0.1'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                  'b0.05-0.1'] + float(bid[1])
    elif 0.06 <= orderbook_percent < 0.065:
        orderbook_df.at[len(orderbook_df) - 1, 'b0.06-0.065'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                    'b0.06-0.065'] + float(bid[1])
        orderbook_df.at[len(orderbook_df) - 1, 'b0.06-0.07'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                   'b0.06-0.07'] + float(bid[1])
        orderbook_df.at[len(orderbook_df) - 1, 'b0.05-0.1'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                  'b0.05-0.1'] + float(bid[1])
    elif 0.065 <= orderbook_percent < 0.07:
        orderbook_df.at[len(orderbook_df) - 1, 'b0.065-0.07'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                    'b0.065-0.07'] + float(bid[1])
        orderbook_df.at[len(orderbook_df) - 1, 'b0.06-0.07'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                   'b0.06-0.07'] + float(bid[1])
        orderbook_df.at[len(orderbook_df) - 1, 'b0.05-0.1'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                  'b0.05-0.1'] + float(bid[1])
    elif 0.07 <= orderbook_percent < 0.075:
        orderbook_df.at[len(orderbook_df) - 1, 'b0.07-0.075'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                    'b0.07-0.075'] + float(bid[1])
        orderbook_df.at[len(orderbook_df) - 1, 'b0.07-0.08'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                   'b0.07-0.08'] + float(bid[1])
        orderbook_df.at[len(orderbook_df) - 1, 'b0.05-0.1'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                  'b0.05-0.1'] + float(bid[1])
    elif 0.075 <= orderbook_percent < 0.08:
        orderbook_df.at[len(orderbook_df) - 1, 'b0.075-0.08'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                    'b0.075-0.08'] + float(bid[1])
        orderbook_df.at[len(orderbook_df) - 1, 'b0.07-0.08'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                   'b0.07-0.08'] + float(bid[1])
        orderbook_df.at[len(orderbook_df) - 1, 'b0.05-0.1'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                  'b0.05-0.1'] + float(bid[1])
    elif 0.08 <= orderbook_percent < 0.085:
        orderbook_df.at[len(orderbook_df) - 1, 'b0.08-0.085'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                    'b0.08-0.085'] + float(bid[1])
        orderbook_df.at[len(orderbook_df) - 1, 'b0.08-0.09'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                   'b0.08-0.09'] + float(bid[1])
        orderbook_df.at[len(orderbook_df) - 1, 'b0.05-0.1'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                  'b0.05-0.1'] + float(bid[1])
    elif 0.085 <= orderbook_percent < 0.09:
        orderbook_df.at[len(orderbook_df) - 1, 'b0.085-0.09'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                    'b0.085-0.09'] + float(bid[1])
        orderbook_df.at[len(orderbook_df) - 1, 'b0.08-0.09'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                   'b0.08-0.09'] + float(bid[1])
        orderbook_df.at[len(orderbook_df) - 1, 'b0.05-0.1'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                  'b0.05-0.1'] + float(bid[1])
    elif 0.09 <= orderbook_percent < 0.095:
        orderbook_df.at[len(orderbook_df) - 1, 'b0.09-0.095'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                    'b0.09-0.095'] + float(bid[1])
        orderbook_df.at[len(orderbook_df) - 1, 'b0.08-0.09'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                   'b0.08-0.09'] + float(bid[1])
        orderbook_df.at[len(orderbook_df) - 1, 'b0.05-0.1'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                  'b0.05-0.1'] + float(bid[1])
    elif 0.095 <= orderbook_percent < 0.1:
        orderbook_df.at[len(orderbook_df) - 1, 'b0.095-0.1'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                   'b0.095-0.1'] + float(bid[1])
        orderbook_df.at[len(orderbook_df) - 1, 'b0.1-0.2'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                 'b0.1-0.2'] + float(bid[1])
        orderbook_df.at[len(orderbook_df) - 1, 'b0.1-0.3'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                 'b0.1-0.3'] + float(bid[1])
    elif 0.1 <= orderbook_percent < 0.12:
        orderbook_df.at[len(orderbook_df) - 1, 'b0.1-0.12'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                  'b0.1-0.12'] + float(bid[1])
        orderbook_df.at[len(orderbook_df) - 1, 'b0.1-0.2'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                 'b0.1-0.2'] + float(bid[1])
        orderbook_df.at[len(orderbook_df) - 1, 'b0.1-0.3'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                 'b0.1-0.3'] + float(bid[1])
    elif 0.12 <= orderbook_percent < 0.15:
        orderbook_df.at[len(orderbook_df) - 1, 'b0.12-0.15'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                   'b0.12-0.15'] + float(bid[1])
        orderbook_df.at[len(orderbook_df) - 1, 'b0.1-0.2'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                 'b0.1-0.2'] + float(bid[1])
        orderbook_df.at[len(orderbook_df) - 1, 'b0.1-0.3'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                 'b0.1-0.3'] + float(bid[1])
    elif 0.15 <= orderbook_percent < 0.2:
        orderbook_df.at[len(orderbook_df) - 1, 'b0.15-0.2'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                  'b0.15-0.2'] + float(bid[1])
        orderbook_df.at[len(orderbook_df) - 1, 'b0.1-0.2'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                 'b0.1-0.2'] + float(bid[1])
        orderbook_df.at[len(orderbook_df) - 1, 'b0.1-0.3'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                 'b0.1-0.3'] + float(bid[1])
    elif 0.2 <= orderbook_percent < 0.25:
        orderbook_df.at[len(orderbook_df) - 1, 'b0.2-0.25'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                  'b0.2-0.25'] + float(bid[1])
        orderbook_df.at[len(orderbook_df) - 1, 'b0.2-0.3'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                 'b0.2-0.3'] + float(bid[1])
        orderbook_df.at[len(orderbook_df) - 1, 'b0.1-0.3'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                 'b0.1-0.3'] + float(bid[1])
    elif 0.25 <= orderbook_percent < 0.3:
        orderbook_df.at[len(orderbook_df) - 1, 'b0.25-0.3'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                  'b0.25-0.3'] + float(bid[1])
        orderbook_df.at[len(orderbook_df) - 1, 'b0.2-0.3'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                 'b0.2-0.3'] + float(bid[1])
        orderbook_df.at[len(orderbook_df) - 1, 'b0.1-0.3'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                 'b0.1-0.3'] + float(bid[1])
    elif 0.3 <= orderbook_percent < 0.35:
        orderbook_df.at[len(orderbook_df) - 1, 'b0.3-0.35'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                  'b0.3-0.35'] + float(bid[1])
        orderbook_df.at[len(orderbook_df) - 1, 'b0.3-0.4'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                 'b0.3-0.4'] + float(bid[1])
        orderbook_df.at[len(orderbook_df) - 1, 'b0.3-0.5'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                 'b0.3-0.5'] + float(bid[1])
    elif 0.35 <= orderbook_percent < 0.4:
        orderbook_df.at[len(orderbook_df) - 1, 'b0.35-0.4'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                  'b0.35-0.4'] + float(bid[1])
        orderbook_df.at[len(orderbook_df) - 1, 'b0.3-0.4'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                 'b0.3-0.4'] + float(bid[1])
        orderbook_df.at[len(orderbook_df) - 1, 'b0.3-0.5'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                 'b0.3-0.5'] + float(bid[1])
    elif 0.4 <= orderbook_percent < 0.45:
        orderbook_df.at[len(orderbook_df) - 1, 'b0.4-0.45'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                  'b0.4-0.45'] + float(bid[1])
        orderbook_df.at[len(orderbook_df) - 1, 'b0.4-0.5'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                 'b0.4-0.5'] + float(bid[1])
        orderbook_df.at[len(orderbook_df) - 1, 'b0.3-0.5'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                 'b0.3-0.5'] + float(bid[1])
    elif 0.45 <= orderbook_percent < 0.5:
        orderbook_df.at[len(orderbook_df) - 1, 'b0.45-0.5'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                  'b0.45-0.5'] + float(bid[1])
        orderbook_df.at[len(orderbook_df) - 1, 'b0.4-0.5'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                 'b0.4-0.5'] + float(bid[1])
        orderbook_df.at[len(orderbook_df) - 1, 'b0.3-0.5'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                 'b0.3-0.5'] + float(bid[1])
    elif 0.5 <= orderbook_percent < 0.6:
        orderbook_df.at[len(orderbook_df) - 1, 'b0.5-0.6'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                 'b0.5-0.6'] + float(bid[1])
        orderbook_df.at[len(orderbook_df) - 1, 'b0.5-0.7'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                 'b0.5-0.7'] + float(bid[1])
        orderbook_df.at[len(orderbook_df) - 1, 'b0.5-1'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.5-1'] + float(
            bid[1])
    elif 0.6 <= orderbook_percent < 0.7:
        orderbook_df.at[len(orderbook_df) - 1, 'b0.6-0.7'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                 'b0.6-0.7'] + float(bid[1])
        orderbook_df.at[len(orderbook_df) - 1, 'b0.5-0.7'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                 'b0.5-0.7'] + float(bid[1])
        orderbook_df.at[len(orderbook_df) - 1, 'b0.5-1'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.5-1'] + float(
            bid[1])
    elif 0.7 <= orderbook_percent < 0.8:
        orderbook_df.at[len(orderbook_df) - 1, 'b0.7-0.8'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                 'b0.7-0.8'] + float(bid[1])
        orderbook_df.at[len(orderbook_df) - 1, 'b0.5-0.7'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                 'b0.5-0.7'] + float(bid[1])
        orderbook_df.at[len(orderbook_df) - 1, 'b0.5-1'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.5-1'] + float(
            bid[1])
    elif 0.7 <= orderbook_percent < 0.8:
        orderbook_df.at[len(orderbook_df) - 1, 'b0.7-0.8'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                 'b0.7-0.8'] + float(bid[1])
        orderbook_df.at[len(orderbook_df) - 1, 'b0.7-1'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.7-1'] + float(
            bid[1])
        orderbook_df.at[len(orderbook_df) - 1, 'b0.5-1'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.5-1'] + float(
            bid[1])
    elif 0.8 <= orderbook_percent < 0.9:
        orderbook_df.at[len(orderbook_df) - 1, 'b0.8-0.9'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                 'b0.8-0.9'] + float(bid[1])
        orderbook_df.at[len(orderbook_df) - 1, 'b0.7-1'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.7-1'] + float(
            bid[1])
        orderbook_df.at[len(orderbook_df) - 1, 'b0.5-1'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.5-1'] + float(
            bid[1])
    elif 0.9 <= orderbook_percent < 1:
        orderbook_df.at[len(orderbook_df) - 1, 'b0.9-1'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.9-1'] + float(
            bid[1])
        orderbook_df.at[len(orderbook_df) - 1, 'b0.7-1'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.7-1'] + float(
            bid[1])
        orderbook_df.at[len(orderbook_df) - 1, 'b0.5-1'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.5-1'] + float(
            bid[1])
    elif orderbook_percent >= 1:
        orderbook_df.at[len(orderbook_df) - 1, 'b1-1+'] = orderbook_df.iloc[len(orderbook_df) - 1]['b1-1+'] + float(
            bid[1])

    return orderbook_df

def fill_asks_value_to_range(ask, orderbook_df, orderbook_percent):
    if orderbook_percent < 0.001:
        orderbook_df.at[len(orderbook_df) - 1, 'a0-0.001'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                 'a0-0.001'] + float(ask[1])
        orderbook_df.at[len(orderbook_df) - 1, 'a0-0.002'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                 'a0-0.002'] + float(ask[1])
        orderbook_df.at[len(orderbook_df) - 1, 'a0-0.005'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                 'a0-0.005'] + float(ask[1])
    elif 0.001 <= orderbook_percent < 0.002:
        orderbook_df.at[len(orderbook_df) - 1, 'a0.001-0.002'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                     'a0.001-0.002'] + float(ask[1])
        orderbook_df.at[len(orderbook_df) - 1, 'a0-0.002'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                 'a0-0.002'] + float(ask[1])
        orderbook_df.at[len(orderbook_df) - 1, 'a0-0.005'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                 'a0-0.005'] + float(ask[1])
    elif 0.002 <= orderbook_percent < 0.003:
        orderbook_df.at[len(orderbook_df) - 1, 'a0.002-0.003'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                     'a0.002-0.003'] + float(ask[1])
        orderbook_df.at[len(orderbook_df) - 1, 'a0.002-0.004'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                     'a0.002-0.004'] + float(ask[1])
        orderbook_df.at[len(orderbook_df) - 1, 'a0-0.005'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                 'a0-0.005'] + float(ask[1])
    elif 0.003 <= orderbook_percent < 0.004:
        orderbook_df.at[len(orderbook_df) - 1, 'a0.003-0.004'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                     'a0.003-0.004'] + float(ask[1])
        orderbook_df.at[len(orderbook_df) - 1, 'a0.002-0.004'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                     'a0.002-0.004'] + float(ask[1])
        orderbook_df.at[len(orderbook_df) - 1, 'a0-0.005'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                 'a0-0.005'] + float(ask[1])
    elif 0.004 <= orderbook_percent < 0.005:
        orderbook_df.at[len(orderbook_df) - 1, 'a0.004-0.005'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                     'a0.001-0.002'] + float(ask[1])
        orderbook_df.at[len(orderbook_df) - 1, 'a0.004-0.006'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                     'a0.004-0.006'] + float(ask[1])
        orderbook_df.at[len(orderbook_df) - 1, 'a0-0.005'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                 'a0-0.005'] + float(ask[1])
    elif 0.005 <= orderbook_percent < 0.006:
        orderbook_df.at[len(orderbook_df) - 1, 'a0.005-0.006'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                     'a0.001-0.002'] + float(ask[1])
        orderbook_df.at[len(orderbook_df) - 1, 'a0.004-0.006'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                     'a0.004-0.006'] + float(ask[1])
        orderbook_df.at[len(orderbook_df) - 1, 'a0.005-0.01'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                    'a0.005-0.01'] + float(ask[1])
    elif 0.006 <= orderbook_percent < 0.007:
        orderbook_df.at[len(orderbook_df) - 1, 'a0.006-0.007'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                     'a0.006-0.007'] + float(ask[1])
        orderbook_df.at[len(orderbook_df) - 1, 'a0.006-0.008'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                     'a0.006-0.008'] + float(ask[1])
        orderbook_df.at[len(orderbook_df) - 1, 'a0.005-0.01'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                    'a0.005-0.01'] + float(ask[1])
    elif 0.007 <= orderbook_percent < 0.008:
        orderbook_df.at[len(orderbook_df) - 1, 'a0.007-0.008'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                     'a0.007-0.008'] + float(ask[1])
        orderbook_df.at[len(orderbook_df) - 1, 'a0.006-0.008'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                     'a0.006-0.008'] + float(ask[1])
        orderbook_df.at[len(orderbook_df) - 1, 'a0.005-0.01'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                    'a0.005-0.01'] + float(ask[1])
    elif 0.008 <= orderbook_percent < 0.009:
        orderbook_df.at[len(orderbook_df) - 1, 'a0.008-0.009'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                     'a0.008-0.009'] + float(ask[1])
        orderbook_df.at[len(orderbook_df) - 1, 'a0.008-0.01'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                    'a0.008-0.01'] + float(ask[1])
        orderbook_df.at[len(orderbook_df) - 1, 'a0.005-0.01'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                    'a0.005-0.01'] + float(ask[1])
    elif 0.009 <= orderbook_percent < 0.01:
        orderbook_df.at[len(orderbook_df) - 1, 'a0.009-0.01'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                    'a0.009-0.01'] + float(ask[1])
        orderbook_df.at[len(orderbook_df) - 1, 'a0.008-0.01'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                    'a0.008-0.01'] + float(ask[1])
        orderbook_df.at[len(orderbook_df) - 1, 'a0.005-0.01'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                    'a0.005-0.01'] + float(ask[1])
    elif 0.01 <= orderbook_percent < 0.015:
        orderbook_df.at[len(orderbook_df) - 1, 'a0.01-0.015'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                    'a0.01-0.015'] + float(ask[1])
        orderbook_df.at[len(orderbook_df) - 1, 'a0.01-0.02'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                   'a0.01-0.02'] + float(ask[1])
        orderbook_df.at[len(orderbook_df) - 1, 'a0.01-0.05'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                   'a0.01-0.05'] + float(ask[1])
    elif 0.015 <= orderbook_percent < 0.02:
        orderbook_df.at[len(orderbook_df) - 1, 'a0.015-0.02'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                    'a0.015-0.02'] + float(ask[1])
        orderbook_df.at[len(orderbook_df) - 1, 'a0.01-0.02'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                   'a0.01-0.02'] + float(ask[1])
        orderbook_df.at[len(orderbook_df) - 1, 'a0.01-0.05'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                   'a0.01-0.05'] + float(ask[1])
    elif 0.02 <= orderbook_percent < 0.025:
        orderbook_df.at[len(orderbook_df) - 1, 'a0.02-0.025'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                    'a0.02-0.025'] + float(ask[1])
        orderbook_df.at[len(orderbook_df) - 1, 'a0.02-0.03'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                   'a0.02-0.03'] + float(ask[1])
        orderbook_df.at[len(orderbook_df) - 1, 'a0.01-0.05'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                   'a0.01-0.05'] + float(ask[1])
    elif 0.025 <= orderbook_percent < 0.03:
        orderbook_df.at[len(orderbook_df) - 1, 'a0.025-0.03'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                    'a0.025-0.03'] + float(ask[1])
        orderbook_df.at[len(orderbook_df) - 1, 'a0.02-0.03'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                   'a0.02-0.03'] + float(ask[1])
        orderbook_df.at[len(orderbook_df) - 1, 'a0.01-0.05'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                   'a0.01-0.05'] + float(ask[1])
    elif 0.03 <= orderbook_percent < 0.035:
        orderbook_df.at[len(orderbook_df) - 1, 'a0.03-0.035'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                    'a0.03-0.035'] + float(ask[1])
        orderbook_df.at[len(orderbook_df) - 1, 'a0.03-0.04'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                   'a0.03-0.04'] + float(ask[1])
        orderbook_df.at[len(orderbook_df) - 1, 'a0.01-0.05'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                   'a0.01-0.05'] + float(ask[1])
    elif 0.035 <= orderbook_percent < 0.04:
        orderbook_df.at[len(orderbook_df) - 1, 'a0.035-0.04'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                    'a0.035-0.04'] + float(ask[1])
        orderbook_df.at[len(orderbook_df) - 1, 'a0.03-0.04'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                   'a0.03-0.04'] + float(ask[1])
        orderbook_df.at[len(orderbook_df) - 1, 'a0.01-0.05'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                   'a0.01-0.05'] + float(ask[1])
    elif 0.04 <= orderbook_percent < 0.045:
        orderbook_df.at[len(orderbook_df) - 1, 'a0.04-0.045'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                    'a0.04-0.045'] + float(ask[1])
        orderbook_df.at[len(orderbook_df) - 1, 'a0.04-0.05'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                   'a0.04-0.05'] + float(ask[1])
        orderbook_df.at[len(orderbook_df) - 1, 'a0.01-0.05'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                   'a0.01-0.05'] + float(ask[1])
    elif 0.045 <= orderbook_percent < 0.05:
        orderbook_df.at[len(orderbook_df) - 1, 'a0.045-0.05'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                    'a0.045-0.05'] + float(ask[1])
        orderbook_df.at[len(orderbook_df) - 1, 'a0.04-0.05'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                   'a0.04-0.05'] + float(ask[1])
        orderbook_df.at[len(orderbook_df) - 1, 'a0.01-0.05'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                   'a0.01-0.05'] + float(ask[1])
    elif 0.05 <= orderbook_percent < 0.055:
        orderbook_df.at[len(orderbook_df) - 1, 'a0.05-0.055'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                    'a0.05-0.055'] + float(ask[1])
        orderbook_df.at[len(orderbook_df) - 1, 'a0.05-0.06'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                   'a0.05-0.06'] + float(ask[1])
        orderbook_df.at[len(orderbook_df) - 1, 'a0.05-0.1'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                  'a0.05-0.1'] + float(ask[1])
    elif 0.055 <= orderbook_percent < 0.06:
        orderbook_df.at[len(orderbook_df) - 1, 'a0.055-0.06'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                    'a0.055-0.06'] + float(ask[1])
        orderbook_df.at[len(orderbook_df) - 1, 'a0.05-0.06'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                   'a0.05-0.06'] + float(ask[1])
        orderbook_df.at[len(orderbook_df) - 1, 'a0.05-0.1'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                  'a0.05-0.1'] + float(ask[1])
    elif 0.06 <= orderbook_percent < 0.065:
        orderbook_df.at[len(orderbook_df) - 1, 'a0.06-0.065'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                    'a0.06-0.065'] + float(ask[1])
        orderbook_df.at[len(orderbook_df) - 1, 'a0.06-0.07'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                   'a0.06-0.07'] + float(ask[1])
        orderbook_df.at[len(orderbook_df) - 1, 'a0.05-0.1'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                  'a0.05-0.1'] + float(ask[1])
    elif 0.065 <= orderbook_percent < 0.07:
        orderbook_df.at[len(orderbook_df) - 1, 'a0.065-0.07'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                    'a0.065-0.07'] + float(ask[1])
        orderbook_df.at[len(orderbook_df) - 1, 'a0.06-0.07'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                   'a0.06-0.07'] + float(ask[1])
        orderbook_df.at[len(orderbook_df) - 1, 'a0.05-0.1'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                  'a0.05-0.1'] + float(ask[1])
    elif 0.07 <= orderbook_percent < 0.075:
        orderbook_df.at[len(orderbook_df) - 1, 'a0.07-0.075'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                    'a0.07-0.075'] + float(ask[1])
        orderbook_df.at[len(orderbook_df) - 1, 'a0.07-0.08'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                   'a0.07-0.08'] + float(ask[1])
        orderbook_df.at[len(orderbook_df) - 1, 'a0.05-0.1'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                  'a0.05-0.1'] + float(ask[1])
    elif 0.075 <= orderbook_percent < 0.08:
        orderbook_df.at[len(orderbook_df) - 1, 'a0.075-0.08'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                    'a0.075-0.08'] + float(ask[1])
        orderbook_df.at[len(orderbook_df) - 1, 'a0.07-0.08'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                   'a0.07-0.08'] + float(ask[1])
        orderbook_df.at[len(orderbook_df) - 1, 'a0.05-0.1'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                  'a0.05-0.1'] + float(ask[1])
    elif 0.08 <= orderbook_percent < 0.085:
        orderbook_df.at[len(orderbook_df) - 1, 'a0.08-0.085'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                    'a0.08-0.085'] + float(ask[1])
        orderbook_df.at[len(orderbook_df) - 1, 'a0.08-0.09'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                   'a0.08-0.09'] + float(ask[1])
        orderbook_df.at[len(orderbook_df) - 1, 'a0.05-0.1'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                  'a0.05-0.1'] + float(ask[1])
    elif 0.085 <= orderbook_percent < 0.09:
        orderbook_df.at[len(orderbook_df) - 1, 'a0.085-0.09'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                    'a0.085-0.09'] + float(ask[1])
        orderbook_df.at[len(orderbook_df) - 1, 'a0.08-0.09'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                   'a0.08-0.09'] + float(ask[1])
        orderbook_df.at[len(orderbook_df) - 1, 'a0.05-0.1'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                  'a0.05-0.1'] + float(ask[1])
    elif 0.09 <= orderbook_percent < 0.095:
        orderbook_df.at[len(orderbook_df) - 1, 'a0.09-0.095'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                    'a0.09-0.095'] + float(ask[1])
        orderbook_df.at[len(orderbook_df) - 1, 'a0.08-0.09'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                   'a0.08-0.09'] + float(ask[1])
        orderbook_df.at[len(orderbook_df) - 1, 'a0.05-0.1'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                  'a0.05-0.1'] + float(ask[1])
    elif 0.095 <= orderbook_percent < 0.1:
        orderbook_df.at[len(orderbook_df) - 1, 'a0.095-0.1'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                   'a0.095-0.1'] + float(ask[1])
        orderbook_df.at[len(orderbook_df) - 1, 'a0.1-0.2'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                 'a0.1-0.2'] + float(ask[1])
        orderbook_df.at[len(orderbook_df) - 1, 'a0.1-0.3'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                 'a0.1-0.3'] + float(ask[1])
    elif 0.1 <= orderbook_percent < 0.12:
        orderbook_df.at[len(orderbook_df) - 1, 'a0.1-0.12'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                  'a0.1-0.12'] + float(ask[1])
        orderbook_df.at[len(orderbook_df) - 1, 'a0.1-0.2'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                 'a0.1-0.2'] + float(ask[1])
        orderbook_df.at[len(orderbook_df) - 1, 'a0.1-0.3'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                 'a0.1-0.3'] + float(ask[1])
    elif 0.12 <= orderbook_percent < 0.15:
        orderbook_df.at[len(orderbook_df) - 1, 'a0.12-0.15'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                   'a0.12-0.15'] + float(ask[1])
        orderbook_df.at[len(orderbook_df) - 1, 'a0.1-0.2'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                 'a0.1-0.2'] + float(ask[1])
        orderbook_df.at[len(orderbook_df) - 1, 'a0.1-0.3'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                 'a0.1-0.3'] + float(ask[1])
    elif 0.15 <= orderbook_percent < 0.2:
        orderbook_df.at[len(orderbook_df) - 1, 'a0.15-0.2'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                  'a0.15-0.2'] + float(ask[1])
        orderbook_df.at[len(orderbook_df) - 1, 'a0.1-0.2'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                 'a0.1-0.2'] + float(ask[1])
        orderbook_df.at[len(orderbook_df) - 1, 'a0.1-0.3'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                 'a0.1-0.3'] + float(ask[1])
    elif 0.2 <= orderbook_percent < 0.25:
        orderbook_df.at[len(orderbook_df) - 1, 'a0.2-0.25'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                  'a0.2-0.25'] + float(ask[1])
        orderbook_df.at[len(orderbook_df) - 1, 'a0.2-0.3'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                 'a0.2-0.3'] + float(ask[1])
        orderbook_df.at[len(orderbook_df) - 1, 'a0.1-0.3'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                 'a0.1-0.3'] + float(ask[1])
    elif 0.25 <= orderbook_percent < 0.3:
        orderbook_df.at[len(orderbook_df) - 1, 'a0.25-0.3'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                  'a0.25-0.3'] + float(ask[1])
        orderbook_df.at[len(orderbook_df) - 1, 'a0.2-0.3'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                 'a0.2-0.3'] + float(ask[1])
        orderbook_df.at[len(orderbook_df) - 1, 'a0.1-0.3'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                 'a0.1-0.3'] + float(ask[1])
    elif 0.3 <= orderbook_percent < 0.35:
        orderbook_df.at[len(orderbook_df) - 1, 'a0.3-0.35'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                  'a0.3-0.35'] + float(ask[1])
        orderbook_df.at[len(orderbook_df) - 1, 'a0.3-0.4'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                 'a0.3-0.4'] + float(ask[1])
        orderbook_df.at[len(orderbook_df) - 1, 'a0.3-0.5'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                 'a0.3-0.5'] + float(ask[1])
    elif 0.35 <= orderbook_percent < 0.4:
        orderbook_df.at[len(orderbook_df) - 1, 'a0.35-0.4'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                  'a0.35-0.4'] + float(ask[1])
        orderbook_df.at[len(orderbook_df) - 1, 'a0.3-0.4'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                 'a0.3-0.4'] + float(ask[1])
        orderbook_df.at[len(orderbook_df) - 1, 'a0.3-0.5'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                 'a0.3-0.5'] + float(ask[1])
    elif 0.4 <= orderbook_percent < 0.45:
        orderbook_df.at[len(orderbook_df) - 1, 'a0.4-0.45'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                  'a0.4-0.45'] + float(ask[1])
        orderbook_df.at[len(orderbook_df) - 1, 'a0.4-0.5'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                 'a0.4-0.5'] + float(ask[1])
        orderbook_df.at[len(orderbook_df) - 1, 'a0.3-0.5'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                 'a0.3-0.5'] + float(ask[1])
    elif 0.45 <= orderbook_percent < 0.5:
        orderbook_df.at[len(orderbook_df) - 1, 'a0.45-0.5'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                  'a0.45-0.5'] + float(ask[1])
        orderbook_df.at[len(orderbook_df) - 1, 'a0.4-0.5'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                 'a0.4-0.5'] + float(ask[1])
        orderbook_df.at[len(orderbook_df) - 1, 'a0.3-0.5'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                 'a0.3-0.5'] + float(ask[1])
    elif 0.5 <= orderbook_percent < 0.6:
        orderbook_df.at[len(orderbook_df) - 1, 'a0.5-0.6'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                 'a0.5-0.6'] + float(ask[1])
        orderbook_df.at[len(orderbook_df) - 1, 'a0.5-0.7'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                 'a0.5-0.7'] + float(ask[1])
        orderbook_df.at[len(orderbook_df) - 1, 'a0.5-1'] = orderbook_df.iloc[len(orderbook_df) - 1]['a0.5-1'] + float(
            ask[1])
    elif 0.6 <= orderbook_percent < 0.7:
        orderbook_df.at[len(orderbook_df) - 1, 'a0.6-0.7'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                 'a0.6-0.7'] + float(ask[1])
        orderbook_df.at[len(orderbook_df) - 1, 'a0.5-0.7'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                 'a0.5-0.7'] + float(ask[1])
        orderbook_df.at[len(orderbook_df) - 1, 'a0.5-1'] = orderbook_df.iloc[len(orderbook_df) - 1]['a0.5-1'] + float(
            ask[1])
    elif 0.7 <= orderbook_percent < 0.8:
        orderbook_df.at[len(orderbook_df) - 1, 'a0.7-0.8'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                 'a0.7-0.8'] + float(ask[1])
        orderbook_df.at[len(orderbook_df) - 1, 'a0.5-0.7'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                 'a0.5-0.7'] + float(ask[1])
        orderbook_df.at[len(orderbook_df) - 1, 'a0.5-1'] = orderbook_df.iloc[len(orderbook_df) - 1]['a0.5-1'] + float(
            ask[1])
    elif 0.7 <= orderbook_percent < 0.8:
        orderbook_df.at[len(orderbook_df) - 1, 'a0.7-0.8'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                 'a0.7-0.8'] + float(ask[1])
        orderbook_df.at[len(orderbook_df) - 1, 'a0.7-1'] = orderbook_df.iloc[len(orderbook_df) - 1]['a0.7-1'] + float(
            ask[1])
        orderbook_df.at[len(orderbook_df) - 1, 'a0.5-1'] = orderbook_df.iloc[len(orderbook_df) - 1]['a0.5-1'] + float(
            ask[1])
    elif 0.8 <= orderbook_percent < 0.9:
        orderbook_df.at[len(orderbook_df) - 1, 'a0.8-0.9'] = orderbook_df.iloc[len(orderbook_df) - 1][
                                                                 'a0.8-0.9'] + float(ask[1])
        orderbook_df.at[len(orderbook_df) - 1, 'a0.7-1'] = orderbook_df.iloc[len(orderbook_df) - 1]['a0.7-1'] + float(
            ask[1])
        orderbook_df.at[len(orderbook_df) - 1, 'a0.5-1'] = orderbook_df.iloc[len(orderbook_df) - 1]['a0.5-1'] + float(
            ask[1])
    elif 0.9 <= orderbook_percent < 1:
        orderbook_df.at[len(orderbook_df) - 1, 'a0.9-1'] = orderbook_df.iloc[len(orderbook_df) - 1]['a0.9-1'] + float(
            ask[1])
        orderbook_df.at[len(orderbook_df) - 1, 'a0.7-1'] = orderbook_df.iloc[len(orderbook_df) - 1]['a0.7-1'] + float(
            ask[1])
        orderbook_df.at[len(orderbook_df) - 1, 'a0.5-1'] = orderbook_df.iloc[len(orderbook_df) - 1]['a0.5-1'] + float(
            ask[1])
    elif orderbook_percent >= 1:
        orderbook_df.at[len(orderbook_df) - 1, 'a1-1+'] = orderbook_df.iloc[len(orderbook_df) - 1]['a1-1+'] + float(
            ask[1])

    return orderbook_df