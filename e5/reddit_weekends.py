import sys
import pandas as pd
import numpy as np
from scipy import stats

OUTPUT_TEMPLATE = (
    "Initial T-test p-value: {initial_ttest_p:.3g}\n"
    "Original data normality p-values: {initial_weekday_normality_p:.3g} {initial_weekend_normality_p:.3g}\n"
    "Original data equal-variance p-value: {initial_levene_p:.3g}\n"
    "Transformed data normality p-values: {transformed_weekday_normality_p:.3g} {transformed_weekend_normality_p:.3g}\n"
    "Transformed data equal-variance p-value: {transformed_levene_p:.3g}\n"
    "Weekly data normality p-values: {weekly_weekday_normality_p:.3g} {weekly_weekend_normality_p:.3g}\n"
    "Weekly data equal-variance p-value: {weekly_levene_p:.3g}\n"
    "Weekly T-test p-value: {weekly_ttest_p:.3g}\n"
    "Mann-Whitney U-test p-value: {utest_p:.3g}"
)


def main():
    reddit_counts = sys.argv[1]
    counts = pd.read_json(reddit_counts, lines=True)
    counts['date'] = pd.to_datetime(counts['date'])

    # keep data in 2012 and 2013
    counts = counts[(counts['date'].dt.year == 2012) | (counts['date'].dt.year == 2013)]
    
    # keep canada subreddit
    counts = counts[counts['subreddit'] == 'canada']

    weekdays = counts.copy()
    weekends = counts.copy()

    weekdays = weekdays.loc[(weekdays['date'].dt.weekday != 5) &  (weekdays['date'].dt.weekday != 6)]
    weekends = weekends.loc[(weekends['date'].dt.weekday == 5) |  (weekends['date'].dt.weekday == 6)]
    
    # ---------------------- T -test / Normality / Equal Variances ---------------
    ttest = stats.ttest_ind(weekdays['comment_count'], weekends['comment_count']).pvalue
    weekdays_norm = stats.normaltest(weekdays['comment_count']).pvalue
    weekends_norm = stats.normaltest(weekends['comment_count']).pvalue
    var = stats.levene(weekdays['comment_count'], weekends['comment_count']).pvalue

    # --------------------- Fix 1: transforming data --------------------------
    '''#weekdays_log = np.log(weekdays['comment_count'])
    #weekends_log = np.log(weekends['comment_count'])
    ttest_trans = stats.ttest_ind(weekdays_log, weekends_log).pvalue
    weekdays_norm_trans = stats.normaltest(weekdays_log).pvalue
    weekends_norm_trans = stats.normaltest(weekends_log).pvalue
    var_trans = stats.levene(weekdays_log, weekends_log).pvalue
    print(ttest_trans, weekdays_norm_trans, weekends_norm_trans, var_trans)'''

    #weekdays_exp = np.exp(weekdays['comment_count'])
    #weekends_exp = np.exp(weekends['comment_count'])

    weekadys_sqrt = np.sqrt(weekdays['comment_count'])
    weekends_sqrt = np.sqrt(weekends['comment_count'])
    ttest_trans = stats.ttest_ind(weekadys_sqrt, weekends_sqrt).pvalue
    weekdays_norm_trans = stats.normaltest(weekadys_sqrt).pvalue
    weekends_norm_trans = stats.normaltest(weekends_sqrt).pvalue
    var_trans = stats.levene(weekadys_sqrt, weekends_sqrt).pvalue
    #print(ttest_trans, weekdays_norm_trans, weekends_norm_trans, var_trans)

    '''weekadys_square = weekdays['comment_count']**2
    weekends_square = weekends['comment_count']**2
    ttest_trans = stats.ttest_ind(weekadys_square, weekends_square).pvalue
    weekdays_norm_trans = stats.normaltest(weekadys_square).pvalue
    weekends_norm_trans = stats.normaltest(weekends_square).pvalue
    var_trans = stats.levene(weekadys_square, weekends_square).pvalue
    print(ttest_trans, weekdays_norm_trans, weekends_norm_trans, var_trans)'''

    # ------------------------------ Fix 2: Central Limit Theorem ----------------
    weekdays['year'] = weekdays['date'].dt.isocalendar().year
    weekdays['week'] = weekdays['date'].dt.isocalendar().week

    weekends['year'] = weekends['date'].dt.isocalendar().year
    weekends['week'] = weekends['date'].dt.isocalendar().week

    weekdays_group = weekdays.groupby(['year','week'])['comment_count'].mean().reset_index()
    weekends_group = weekends.groupby(['year','week'])['comment_count'].mean().reset_index()
    #print(weekdays_group)

    ttest_CLT = stats.ttest_ind(weekdays_group['comment_count'], weekends_group['comment_count']).pvalue
    weekdays_norm_CLT = stats.normaltest(weekdays_group['comment_count']).pvalue
    weekends_norm_CLT = stats.normaltest(weekends_group['comment_count']).pvalue
    var_CLT = stats.levene(weekdays_group['comment_count'], weekends_group['comment_count']).pvalue
    #print(ttest_CLT, weekdays_CLT, weekends_CLT, var_CLT)

    # ------------------------- Fix 3: non-parametric test ------------------
    u_test = stats.mannwhitneyu(weekdays['comment_count'], weekends['comment_count'], alternative="two-sided").pvalue
    #print(Utest)

    print(OUTPUT_TEMPLATE.format(
        initial_ttest_p=ttest,
        initial_weekday_normality_p=weekdays_norm,
        initial_weekend_normality_p=weekends_norm,
        initial_levene_p=var,
        transformed_weekday_normality_p=weekdays_norm_trans,
        transformed_weekend_normality_p=weekends_norm_trans,
        transformed_levene_p=var_trans,
        weekly_weekday_normality_p=weekdays_norm_CLT,
        weekly_weekend_normality_p=weekends_norm_CLT,
        weekly_levene_p=var_CLT,
        weekly_ttest_p=ttest_CLT,
        utest_p=u_test,
    ))


if __name__ == '__main__':
    main()
