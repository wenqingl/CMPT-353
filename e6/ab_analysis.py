import sys
import pandas as pd
from scipy.stats import chi2_contingency
from scipy.stats import mannwhitneyu


OUTPUT_TEMPLATE = (
    '"Did more/less users use the search feature?" p-value:  {more_users_p:.3g}\n'
    '"Did users search more/less?" p-value:  {more_searches_p:.3g} \n'
    '"Did more/less instructors use the search feature?" p-value:  {more_instr_p:.3g}\n'
    '"Did instructors search more/less?" p-value:  {more_instr_searches_p:.3g}'
)


def main():
    searchdata_file = sys.argv[1]
    searchdata = pd.read_json(searchdata_file, orient='records', lines=True)

    new = searchdata.copy()
    old = searchdata.copy()

    new = new[new['uid'] % 2 != 0]
    old = old[old['uid'] % 2 == 0]
    

    new_search = new[new['search_count'] > 0]
    new_nonsearch = new[new['search_count'] <= 0]
    new_search_ins = new_search[new_search['is_instructor'] == True]
    new_nonsearch_ins = new_nonsearch[new_nonsearch['is_instructor'] == True]

    old_search = old[old['search_count'] > 0]
    old_nonsearch = old[old['search_count'] <= 0]
    old_search_ins = old_search[old_search['is_instructor'] == True]
    old_nonsearch_ins = old_nonsearch[old_nonsearch['is_instructor'] == True]

    count = [[new_search.shape[0], new_nonsearch.shape[0]],
             [old_search.shape[0], old_nonsearch.shape[0]]]

    chi21, p1, dof1, ex1 = chi2_contingency(count)

    ins = [[new_search_ins.shape[0], new_nonsearch_ins.shape[0]], 
           [old_search_ins.shape[0], old_nonsearch_ins.shape[0]]]
    chi23, p3, dof3, ex3 = chi2_contingency(ins)

    new_ins = new[new['is_instructor'] == True]
    old_ins = old[old['is_instructor'] == True]
    p2 = mannwhitneyu(new['search_count'], old['search_count']).pvalue
    p4 = mannwhitneyu(new_ins['search_count'], old_ins['search_count']).pvalue

    # ...

    # Output
    print(OUTPUT_TEMPLATE.format(
        more_users_p=p1,
        more_searches_p=p2,
        more_instr_p=p3,
        more_instr_searches_p=p4,
    ))


if __name__ == '__main__':
    main()
