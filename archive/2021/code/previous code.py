"""# Search for previously transferred data and open shapefile as a geo-dataframe.
    previous_gdf = previous_data_check_fn(transition_dir, year, feature_type)"""

    """print('P'*50)
    # check if the previous geo-dataframe is empty
    #if previous_gdf != 'BLANK':
    if len(previous_gdf.index) == 0:
        print('No previous transition data exists.....')
        # final_gdf_list.append(gdf)
        # gdf['DELETE'] = 0
        print('gdf: ', gdf)
        previous_transition_list.append(gdf)
        final_transition_list.append(gdf)
        previous_transition_data = False"""

    else:
        """print('previous_gdf 446: ', previous_gdf.info())
        previous_transition_data = True
        #test_df, previous_df = convert_to_df_fn(previous_gdf, gdf)

        print('test_df 449: ', test_df.info())
        print('previous_df 450: ', previous_df.info())
        print('Filter the test and previous data:')"""
        # for loop through test dataframe based on property name.
        """for prop_ in test_df.PROPERTY.unique():
            print(' - Property name: ', prop_)
            # filter dataframe based on property name.
            filter_test_df = test_df[test_df['PROPERTY'] == prop_]"""

            """# export all previous data that does not contain the property names contained in the test data.
            prop_filter_previous = previous_df[previous_df['PROPERTY'] == prop_]

            if len(prop_filter_previous.index) > 0:
                print('previous greater than '*50)
                input_left_df = compare_previous_dataframes_fn(previous_df, prop_filter_previous)
                print('input_left_df: ', input_left_df)
                if len(input_left_df) == 0:
                    #previous_df and prop_filter_previous both have the same data.
                    delete_previous_list.append(prop_filter_previous)
                else:
                    pass"""

            '''if len(filter_test_df.index) > 0:
                print('- Number of observations: ', len(filter_test_df.index))

                # for loop through filtered test dataframe based on currency date.
                for currency in filter_test_df.DATE_CURR_2.unique():

                    # calculate + and - 15 days from today's date (currency date).
                    start_date, end_date = thirty_day_currency_range_fn(currency)

                    # filter test data by currency value in the for loop and the previous data by the start and end dates.
                    prop_curr_test_df, filter_previous_df, keep_previous_df = filter_data_currency_fn(
                        start_date, end_date, filter_test_df, previous_df, currency, prop_)

                    # append previous data that is property specific that falls outside of the currency date start and end dates.
                    keep_previous_list.append(keep_previous_df)'''

                    if len(filter_previous_df.index) > 0:
                        print('+'*50)
                        print(' length of previous with previous and currency filter: ', len(filter_previous_df.index))
                        print('+' * 50)

                        # filter the
                        curr_date_previous_df = filter_previous_data_latest_currency_date_fn(
                            filter_previous_df, previous_df)

                        if len(curr_date_previous_df.index) > 0:
                            print(' - There are observations: ', len(filter_previous_df.index))
                            # Previous data exists
                            print('-' * 50)
                            print('Checking that the data is new.... line 512')
                            input_left_df, previous_left_df = compare_dataframes_fn(curr_date_previous_df,
                                                                                    prop_curr_test_df)

                            print('input_left_df: ', len(input_left_df.index), ' previous_left_df: ',
                                  len(previous_left_df.index))
                            print('input_left_df: ', input_left_df)

                            if len(input_left_df.index) == 0 and len(previous_left_df.index) == 0:

                                print(' - Data already exists in the transition directory. line 587')
                                print('-' * 50)
                                previous_transition_list.append(prop_curr_test_df)


                            elif len(input_left_df.index) > len(previous_left_df.index):
                                print('The TEST data contains ',
                                      str(len(input_left_df.index) - len(previous_left_df.index)),
                                      ' more observations than the PREVIOUS data.')
                                print('len(input_left_df.index): ', len(input_left_df.index))
                                print('len(previous_left_df.index): ', len(previous_left_df.index))
                                # delete_list.append(curr_date_previous_df)
                                # delete_prev_list.append(prop_curr_test_df)
                                """final_gdf_list.append(prop_curr_test_df)
                                prop_curr_test_df['DELETE'] = 0
                                add_to_previous_list.append(prop_curr_test_df)
                                curr_date_previous_df['DELETE'] = 1"""
                                """delete_from_previous_list.append(curr_date_previous_df)"""
                                # prop_curr_test_df['DELETE'] = 0
                                previous_transition_list.append(prop_curr_test_df)
                                final_transition_list.append(prop_curr_test_df)
                                # print('keep_gdf 524: ', prop_curr_test_df)
                                # curr_date_previous_df['DELETE'] = 1
                                # previous_transition_list.append(curr_date_previous_df)
                                # print('delete_gdf 527: ', curr_date_previous_df)
                                print('-' * 50)

                            elif len(input_left_df.index) < len(previous_left_df.index):
                                print('The PREVIOUS data contains ',
                                      str(len(previous_left_df.index) - len(input_left_df.index)),
                                      ' more observations than the TEST data.')
                                print('len(input_left_df.index): ', len(input_left_df.index))
                                print('len(previous_left_df.index): ', len(previous_left_df.index))
                                # delete_list.append(curr_date_previous_df)
                                # delete_prev_list.append(prop_curr_test_df)
                                # final_gdf_list.append(prop_curr_test_df)
                                """curr_date_previous_df['DELETE'] = 0
                                add_to_previous_list.append(curr_date_previous_df)
                                prop_curr_test_df['DELETE'] = 1
                                delete_from_previous_list.append(prop_curr_test_df)"""

                                # curr_date_previous_df['DELETE'] = 0
                                # previous_transition_list.append(curr_date_previous_df)
                                # print('keep_gdf 546: ', curr_date_previous_df)
                                # prop_curr_test_df['DELETE'] = 1
                                previous_transition_list.append(prop_curr_test_df)
                                # print('delete_gdf 549: ', prop_curr_test_df)

                                print('-' * 50)

                        else:
                            print('line 584 - else')
                            # prop_curr_test_df['DELETE'] = 0
                            previous_transition_list.append(prop_curr_test_df)
                            print('add_gdf 556: ', prop_curr_test_df)

                    else:
                        # prop_curr_test_df['DELETE'] = 0
                        previous_transition_list.append(prop_curr_test_df)
                        print('add_gdf 561: ', prop_curr_test_df)

        '''else:
            print('No previous transition data exists.....')
            # final_gdf_list.append(gdf)
            # gdf['DELETE'] = 0
            previous_transition_list.append(gdf)'''

    print('previous_transition_list: ', previous_transition_list)

'''for i in for_migration_path_list:
    while os.path.exists(i):


        if not os.listdir(i):
            value = 'Continue'
            pass
        else:
            print('ERROR !!!!!!')
            print('You have files in the FOR_MIGRATION directory...')
            print(' - this directory must be empty to run this script.')
            print('-'*50)
            print('-' * 50)
            print('Do you want me to purge the FOR_MIGRATION directory for you???????')
            value = input('Y or N (press enter)')
            pass

if value == 'Y':
    print('Purge directory')

    path, _ = i.rsplit('\\', 1)
    shutil.rmtree(path)
    print('Deleting......')
    print(' - Directory: ', path)

elif value == 'N':
    print('You selected N')
    print(' - Goodbye')
    sys.exit()

else:'''