
#===================================================================================================
#
# Name:       wb_load
#
# Purpose:    loads the raw waybill data to waybill_full table
#
# Author:     Gary Baker
#
# Version:    1.0 - 10/28/2016
# Version:    1.1 - 03/08/2017 - updated to run off of gdb instead of shapefiles
# Version:    1.2 - 03/30/2017 - further layer name standardization, spatialref standardization, 
#                                and network connectivity checks
# Version:    1.3 - 05/01/2017 - added flow rights and additional startup checks
# Version:    1.4 - 06/23/2017 - distance checks and net checks for all RRs
#
# ==================================================================================================
# IMPORT REQUIRED MODULES
# ==================================================================================================

from __future__ import print_function

import os, sys
import datetime
import logging
import pypyodbc

import wb_utils as wbutl

# ==================================================================================================
# WAYBILL FIELD SPECS
# ==================================================================================================

# This is not maintained in a separate text file because any changes to this would likely require
# changes to the program.

# waybill field num, subset field, field name, num positions, type, cols, sql type, sql nullable,
# desc

# TODO convert waybill_date to smalldatetime
# TODO convert deregulation_date to smalldatetime
# NOTE there is a bit of an inconsistency between "dest" and "term"

WAYBILL_FIELD_SPECS = """
  1  1  serial_num                         6      N        1-6   INT             NOT NULL  Unique Serial Number
  2  1  waybill_num                        6      N       7-12   INT             NOT NULL  Waybill Number
  3  0  waybill_date                       8      N      13-20   VARCHAR(8)      NOT NULL  Waybill Date (mmddccyy)
  4  0  acct_period                        6      N      21-26   VARCHAR(8)      NOT NULL  Accounting Period (mmccyy)
  5  1  num_carload                        4      N      27-30   INT             NOT NULL  Number of Carloads
  6  0  car_initials                       4      A      31-34   VARCHAR(4)      NOT NULL  Car Initial
  7  1  car_number                         6      N      35-40   INT             NOT NULL  Car Number
  8  1  tofc_service_code                  3    A/N      41-43   VARCHAR(3)          NULL  Intermodal TOFC/COFC Service Code
  9  1  num_tofc_units                     4      N      44-47   INT             NOT NULL  Number of TOFC/COFCs
 10  0  tofc_initial                       4      A      48-51   VARCHAR(4)          NULL  TOFC/COFC Initial
 11  0  tofc_number                        6      N      52-57   INT             NOT NULL  TOFC/COFC Number
 12  1  stcc_w49                           7      N      58-64   VARCHAR(7)      NOT NULL  Commodity Code (STCC)
 13  0  billed_weight                      9      N      65-73   INT             NOT NULL  Billed Weight
 14  0  actual_weight                      9      N      74-82   INT             NOT NULL  Actual Weight
 15  0  freight_revenue                    9      N      83-91   INT             NOT NULL  Freight Revenue
 16  0  transit_charges                    9      N     92-100   INT             NOT NULL  Transit Charges
 17  0  misc_charges                       9      N    101-109   INT             NOT NULL  Miscellaneous Charges
 18  0  inter_intra_state_code             1      N        110   INT             NOT NULL  Inter/Intra State Code
 19  0  transit_code                       1      N        111   INT             NOT NULL  Transit Code
 20  0  all_rail_intermodal_code           1      N        112   INT             NOT NULL  All Rail/Intermodal Code
 21  0  type_move                          1      N        113   INT             NOT NULL  Type Move (import/export)
 22  0  type_move_via_water                1      N        114   INT             NOT NULL  Type Move Via Water
 23  0  sub_truck_for_rail                 1      N        115   INT             NOT NULL  Substituted Truck for Rail
 24  1  shortline_miles                    4      N    116-119   INT             NOT NULL  Shortline Miles
 25  0  rebill_code                        1      N        120   INT             NOT NULL  Rebill Code
 26  0  stratum_id                         1      N        121   INT             NOT NULL  Stratum Identification
 27  0  subsample_code                     1      N        122   INT             NOT NULL  Subsample Code
 28  0  intermodal_equip_flag              1      N        123   INT             NOT NULL  Intermodal Equipment Flag
 29  0  calculated_rate_flg                1      N        124   INT             NOT NULL  Calculated Rate Flag
 30  0  waybill_identifier                25    A/N    125-149   VARCHAR(25)         NULL  Waybill Identifier (MRI only)
 31  1  reporting_rr                       3      N    150-152   INT             NOT NULL  Reporting Railroad
 32  1  orig_fsac                          5      N    153-157   VARCHAR(5)      NOT NULL  Origin FSAC
 33  1  orig_rr                            3      N    158-160   INT             NOT NULL  Origin Railroad
 34  1  intrchng_1_rule_260                5      A    161-165   VARCHAR(5)          NULL  Interchange #1 Rule 260
 35  0  bridge_1_rr                        3      N    166-168   INT                 NULL  First Bridge RR
 36  1  intrchng_2_rule_260                5      A    169-173   VARCHAR(5)          NULL  Interchange #2 Rule 260
 37  0  bridge_2_rr                        3      N    174-176   INT                 NULL  Second Bridge RR
 38  1  intrchng_3_rule_260                5      A    177-181   VARCHAR(5)          NULL  Interchange #3 Rule 260
 39  0  bridge_3_rr                        3      N    182-184   INT                 NULL  Third Bridge RR
 40  1  intrchng_4_rule_260                5      A    185-189   VARCHAR(5)          NULL  Interchange #4 Rule 260
 41  0  bridge_4_rr                        3      N    190-192   INT                 NULL  Fourth Bridge RR
 42  1  intrchng_5_rule_260                5      A    193-197   VARCHAR(5)          NULL  Interchange #5 Rule 260
 43  0  bridge_5_rr                        3      N    198-200   INT                 NULL  Fifth Bridge RR
 44  1  intrchng_6_rule_260                5      A    201-205   VARCHAR(5)          NULL  Interchange #6 Rule 260
 45  0  bridge_6_rr                        3      N    206-208   INT                 NULL  Sixth Bridge RR
 46  1  intrchng_7_rule_260                5      A    209-213   VARCHAR(5)          NULL  Interchange #7 Rule 260
 51  1  term_rr                            3      N    214-216   INT             NOT NULL  Termination Railroad
 52  1  term_fsac                          5      N    217-221   VARCHAR(5)      NOT NULL  Termination FSAC
 53  0  population_count                   8      N    222-229   INT             NOT NULL  Population Count
 54  0  stratum_count                      6      N    230-235   INT             NOT NULL  Stratum Count
 55  0  reporting_period_len               1      N        236   INT             NOT NULL  Reporting Period Length
 56  0  car_owners_mark                    4      A    237-240   VARCHAR(4)          NULL  Car Owner-s Mark
 57  0  car_lessees_mark                   4      A    241-244   VARCHAR(4)          NULL  Car Lessee-s Mark
 58  0  car_capacity                       5      N    245-249   INT             NOT NULL  Car Capacity
 59  0  nominal_car_capacity               3      N    250-252   INT             NOT NULL  Nominal Car Capacity - Expired
 60  0  tare_wgt_of_car                    4      N    253-256   INT             NOT NULL  Tare Weight of Car
 61  0  outside_length                     5      N    257-261   INT             NOT NULL  Outside Length
 62  0  outside_width                      4      N    262-265   INT             NOT NULL  Outside Width
 63  0  outside_height                     4      N    266-269   INT             NOT NULL  Outside Height
 64  0  extreme_outside_height             4      N    270-273   INT             NOT NULL  Extreme Outside Height
 65  0  type_wheel                         1      A        274   VARCHAR(1)      NOT NULL  Type of Wheel Bearings and Brakes
 66  0  num_axles                          1    A/N        275   VARCHAR(1)      NOT NULL  Number of Axles
 67  0  draft_gear                         2      N    276-277   INT             NOT NULL  Draft Gear
 68  0  articulated_units                  1      N        278   INT             NOT NULL  Number of Articulated Units
 69  0  pool_code_num                      7      N    279-285   INT             NOT NULL  Pool Code Number
 70  0  aar_equip_type                     4    A/N    286-289   VARCHAR(4)      NOT NULL  AAR Equipment Type
 71  0  mech_designation                   4      A    290-293   VARCHAR(4)      NOT NULL  Mechanical Designation Code
 72  0  licensing_state_tofc               2      A    294-295   VARCHAR(2)          NULL  Licensing State (TOFC)
 73  0  max_weight_on_rail                 3      N    296-298   INT             NOT NULL  Maximum Weight on Rail
 74  1  orig_splc                          6      A    299-304   VARCHAR(6)      NOT NULL  Origin SPLC
 75  1  dest_splc                          6      A    305-310   VARCHAR(6)      NOT NULL  Destination SPLC
 76  1  stcc_without_haz_codes             7      A    311-317   VARCHAR(7)      NOT NULL  STCC w/o Hazardous (49) Codes
 77  1  orig_rr_alpha                      4      A    318-321   VARCHAR(4)      NOT NULL  Origin Railroad Alpha
 78  1  intrchng_rr_1_alpha                4      A    322-325   VARCHAR(4)          NULL  First Interchange RR Alpha
 79  1  intrchng_rr_2_alpha                4      A    326-329   VARCHAR(4)          NULL  Second Interchange RR Alpha
 80  1  intrchng_rr_3_alpha                4      A    330-333   VARCHAR(4)          NULL  Third Interchange RR Alpha
 81  1  intrchng_rr_4_alpha                4      A    334-337   VARCHAR(4)          NULL  Fourth Interchange RR Alpha
 82  1  intrchng_rr_5_alpha                4      A    338-341   VARCHAR(4)          NULL  Fifth Interchange RR Alpha
 83  1  intrchng_rr_6_alpha                4      A    342-345   VARCHAR(4)          NULL  Sixth Interchange RR Alpha
 86  1  term_rr_alpha                      4      A    346-349   VARCHAR(4)      NOT NULL  Termination Railroad Alpha
 87  0  junc_freq                          1      N        350   INT             NOT NULL  Junction Frequency
 88  0  theoretical_exp_factor             3      N    351-353   INT             NOT NULL  Theoretical Expansion Factor
 89  0  routing_error_flag                 1      A        354   VARCHAR(1)      NOT NULL  Routing Error Flag
 90  0  stb_car_type                       2      N    355-356   INT             NOT NULL  STB Car Type
 92  0  error_code1                        2      N    357-358   INT             NOT NULL  AAR/RAILINC Error Codes
 92  0  error_code2                        2      N    359-360   INT             NOT NULL  AAR/RAILINC Error Codes
 92  0  error_code3                        2      N    361-362   INT             NOT NULL  AAR/RAILINC Error Codes
 93  0  car_ownership                      1      A        363   VARCHAR(1)      NOT NULL  Car Ownership Category
 94  0  aar_tofc_unit_type                 4    A/N    364-367   VARCHAR(4)          NULL  AAR Trailer/Container Equipment Type Code
 95  0  deregulation_date                  8      N    368-375   VARCHAR(8)          NULL  Deregulation Date (ccyymmdd)
 96  0  deregulation_flag                  1      A        376   VARCHAR(1)      NOT NULL  Deregulation Flag
 97  0  service_type                       1      N        377   VARCHAR(1)      NOT NULL  Service Type
 98  1  expanded_carloads                  6      N    378-383   INT             NOT NULL  Expanded Carloads
 99  0  billed_weight_in_tons              7      N    384-390   INT             NOT NULL  Billed Weight in Tons
100  1  expanded_tons                      8      N    391-398   INT             NOT NULL  Expanded Tons
101  0  expanded_tofc_count                6      N    399-404   INT             NOT NULL  Expanded Trailer/Container Count
102  0  expanded_total_revenue            10      N    405-414   INT             NOT NULL  Expanded Total Revenue
103  0  orig_rr_split_revenue             10      N    415-424   INT             NOT NULL  Origin Railroad Split Revenue
104  0  intrchng_rr_split_revenue_1       10      N    425-434   INT                 NULL  First Interchange RR Split Revenue
105  0  intrchng_rr_split_revenue_2       10      N    435-444   INT                 NULL  Second Interchange RR Split Revenue
106  0  intrchng_rr_split_revenue_3       10      N    445-454   INT                 NULL  Third Interchange RR Split Revenue
107  0  intrchng_rr_split_revenue_4       10      N    455-464   INT                 NULL  Fourth Interchange RR Split Revenue
108  0  intrchng_rr_split_revenue_5       10      N    465-474   INT                 NULL  Fifth Interchange RR Split Revenue
109  0  intrchng_rr_split_revenue_6       10      N    475-484   INT                 NULL  Sixth Interchange RR Split Revenue
112  0  term_rr_split_revenue             10      N    485-494   INT             NOT NULL  Termination Railroad Split Revenue
113  0  rr_dist_1                          5      N    495-499   FLOAT           NOT NULL  First Railroad Distance
114  0  rr_dist_2                          5      N    500-504   FLOAT           NOT NULL  Second Railroad Distance
115  0  rr_dist_3                          5      N    505-509   FLOAT           NOT NULL  Third Railroad Distance
116  0  rr_dist_4                          5      N    510-514   FLOAT           NOT NULL  Fourth Railroad Distance
117  0  rr_dist_5                          5      N    515-519   FLOAT           NOT NULL  Fifth Railroad Distance
118  0  rr_dist_6                          5      N    520-524   FLOAT           NOT NULL  Sixth Railroad Distance
119  0  rr_dist_7                          5      N    525-529   FLOAT           NOT NULL  Seventh Railroad Distance
122  0  term_rr_dist                       5      N    530-534   FLOAT           NOT NULL  Termination Railroad Distance
123  0  total_distance                     5      N    535-539   FLOAT           NOT NULL  Total Distance
124  1  orig_state                         2      A    540-541   VARCHAR(2)      NOT NULL  Origin State Alpha
125  0  junc_state_1                       2      A    542-543   VARCHAR(2)          NULL  First Junction State Alpha
126  0  junc_state_2                       2      A    544-545   VARCHAR(2)          NULL  Second Junction State Alpha
127  0  junc_state_3                       2      A    546-547   VARCHAR(2)          NULL  Third Junction State Alpha
128  0  junc_state_4                       2      A    548-549   VARCHAR(2)          NULL  Fourth Junction State Alpha
129  0  junc_state_5                       2      A    550-551   VARCHAR(2)          NULL  Fifth Junction State Alpha
130  0  junc_state_6                       2      A    552-553   VARCHAR(2)          NULL  Sixth Junction State Alpha
131  0  junc_state_7                       2      A    554-555   VARCHAR(2)          NULL  Seventh Junction State Alpha
134  1  term_state                         2      A    556-557   VARCHAR(2)      NOT NULL  Termination State Alpha
135  0  orig_bea_area                      3      N    558-560   INT             NOT NULL  Origin BEA Area
136  0  term_bea_area                      3      N    561-563   INT             NOT NULL  Termination BEA Area
137  1  orig_fips                          5      N    564-568   INT             NOT NULL  Origin FIPS Code
138  1  term_fips                          5      N    569-573   INT             NOT NULL  Termination FIPS Code
139  0  orig_freight_area                  2      N    574-575   INT             NOT NULL  Origin Freight Area
140  0  term_freight_area                  2      N    576-577   INT             NOT NULL  Termination Freight Area
141  0  orig_freight_territory             1      N        578   INT             NOT NULL  Origin Freight Territory
142  0  term_freight_territory             1      N        579   INT             NOT NULL  Termination Freight Territory
143  0  orig_smsa                          4      N    580-583   INT             NOT NULL  Origin SMSA
144  0  term_smsa                          4      N    584-587   INT             NOT NULL  Termination SMSA
145  0  orig_net3_num                      5      N    588-592   INT             NOT NULL  Origin NET3 Number
146  0  jct_net3_num_1                     5      N    593-597   INT                 NULL  First Junction NET3 Number
147  0  jct_net3_num_2                     5      N    598-602   INT                 NULL  Second Junction NET3 Number
148  0  jct_net3_num_3                     5      N    603-607   INT                 NULL  Third Junction NET3 Number
149  0  jct_net3_num_4                     5      N    608-612   INT                 NULL  Fourth Junction NET3 Number
150  0  jct_net3_num_5                     5      N    613-617   INT                 NULL  Fifth Junction NET3 Number
151  0  jct_net3_num_6                     5      N    618-622   INT                 NULL  Sixth Junction NET3 Number
152  0  jct_net3_num_7                     5      N    623-627   INT                 NULL  Seventh Junction NET3 Number
155  0  term_net3_num                      5      N    628-632   INT                 NULL  Termination NET3 Number
156  0  al_flag                            1      N        633   INT             NOT NULL  Alabama
156  0  ar_flag                            1      N        634   INT             NOT NULL  Arizona
156  0  az_flag                            1      N        635   INT             NOT NULL  Arkansas
156  0  ca_flag                            1      N        636   INT             NOT NULL  California
156  0  co_flag                            1      N        637   INT             NOT NULL  Colorado
156  0  ct_flag                            1      N        638   INT             NOT NULL  Connecticut
156  0  de_flag                            1      N        639   INT             NOT NULL  Delaware
156  0  dc_flag                            1      N        640   INT             NOT NULL  District of Columbia
156  0  fl_flag                            1      N        641   INT             NOT NULL  Florida
156  0  ga_flag                            1      N        642   INT             NOT NULL  Georgia
156  0  id_flag                            1      N        643   INT             NOT NULL  Idaho
156  0  il_flag                            1      N        644   INT             NOT NULL  Illinois
156  0  in_flag                            1      N        645   INT             NOT NULL  Indiana
156  0  ia_flag                            1      N        646   INT             NOT NULL  Iowa
156  0  ks_flag                            1      N        647   INT             NOT NULL  Kansas
156  0  ky_flag                            1      N        648   INT             NOT NULL  Kentucky
156  0  la_flag                            1      N        649   INT             NOT NULL  Louisiana
156  0  me_flag                            1      N        650   INT             NOT NULL  Maine
156  0  md_flag                            1      N        651   INT             NOT NULL  Maryland
156  0  ma_flag                            1      N        652   INT             NOT NULL  Massachusetts
156  0  mi_flag                            1      N        653   INT             NOT NULL  Michigan
156  0  mn_flag                            1      N        654   INT             NOT NULL  Minnesota
156  0  ms_flag                            1      N        655   INT             NOT NULL  Mississippi
156  0  mo_flag                            1      N        656   INT             NOT NULL  Missouri
156  0  mt_flag                            1      N        657   INT             NOT NULL  Montana
156  0  ne_flag                            1      N        658   INT             NOT NULL  Nebraska
156  0  nv_flag                            1      N        659   INT             NOT NULL  Nevada
156  0  nh_flag                            1      N        660   INT             NOT NULL  New Hampshire
156  0  nj_flag                            1      N        661   INT             NOT NULL  New Jersey
156  0  nm_flag                            1      N        662   INT             NOT NULL  New Mexico
156  0  ny_flag                            1      N        663   INT             NOT NULL  New York
156  0  nc_flag                            1      N        664   INT             NOT NULL  North Carolina
156  0  nd_flag                            1      N        665   INT             NOT NULL  North Dakota
156  0  oh_flag                            1      N        666   INT             NOT NULL  Ohio
156  0  ok_flag                            1      N        667   INT             NOT NULL  Oklahoma
156  0  or_flag                            1      N        668   INT             NOT NULL  Oregon
156  0  pa_flag                            1      N        669   INT             NOT NULL  Pennsylvania
156  0  ri_flag                            1      N        670   INT             NOT NULL  Rhode Island
156  0  sc_flag                            1      N        671   INT             NOT NULL  South Carolina
156  0  sd_flag                            1      N        672   INT             NOT NULL  South Dakota
156  0  tn_flag                            1      N        673   INT             NOT NULL  Tennessee
156  0  tx_flag                            1      N        674   INT             NOT NULL  Texas
156  0  ut_flag                            1      N        675   INT             NOT NULL  Utah
156  0  vt_flag                            1      N        676   INT             NOT NULL  Vermont
156  0  va_flag                            1      N        677   INT             NOT NULL  Virginia
156  0  wa_flag                            1      N        678   INT             NOT NULL  Washington
156  0  wv_flag                            1      N        679   INT             NOT NULL  West Virginia
156  0  wi_flag                            1      N        680   INT             NOT NULL  Wisconsin
156  0  wy_flag                            1      N        681   INT             NOT NULL  Wyoming
156  0  cd_flag                            1      N        682   INT             NOT NULL  Canada
156  0  mx_flag                            1      N        683   INT             NOT NULL  Mexico
156  0  other_state_flag                   1      N        684   INT             NOT NULL  All Other
157  0  international_harmonized_code     12      A    685-696   VARCHAR(12)     NOT NULL  International Harmonized Code
158  0  standard_industrial_classific      4      A    697-700   VARCHAR(4)      NOT NULL  Standard Industrial Classification
159  0  international_sic                  4      A    701-704   VARCHAR(4)      NOT NULL  International S. I. C.
160  0  dominion_of_canada_code            3      A    705-707   VARCHAR(3)      NOT NULL  Dominion of Canada Code
161  0  cs_54_group_code                   2      A    708-709   VARCHAR(2)      NOT NULL  CS54 Group Code
162  0  orig_freight_stn_type              4      A    710-713   VARCHAR(4)      NOT NULL  Origin Freight Station Type
163  0  dest_freight_stn_type              4      A    714-717   VARCHAR(4)      NOT NULL  Destination Freight Station Type
164  0  orig_freight_stn_rating_zip        9      A    718-726   VARCHAR(9)      NOT NULL  Origin Freight Station Rating ZIP
165  0  dest_freight_stn_rating_zip        9      A    727-735   VARCHAR(9)      NOT NULL  Dest. Freight Station Rating ZIP
166  0  orig_rate_base_splc                9      A    736-744   VARCHAR(9)      NULL      Origin Rate Base SPLC
167  0  dest_rate_base_splc                9      A    745-753   VARCHAR(9)      NULL      Destination Rate Base SPLC
168  0  orig_switch_limit_splc             9      A    754-762   VARCHAR(9)      NULL      Origin Switch Limit SPLC
169  0  dest_switch_limit_splc             9      A    763-771   VARCHAR(9)      NULL      Destination Switch Limit SPLC
170  0  orig_customs_flag                  1      A        772   VARCHAR(1)      NOT NULL  Origin Customs Flag
171  0  dest_customs_flag                  1      A        773   VARCHAR(1)      NOT NULL  Destination Customs Flag
172  0  orig_grain_flag                    1      A        774   VARCHAR(1)      NOT NULL  Origin Grain Flag
173  0  dest_grain_flag                    1      A        775   VARCHAR(1)      NOT NULL  Destination Grain Flag
174  0  orig_auto_ramp_facility_code       1      A        776   VARCHAR(1)      NOT NULL  Origin Automobile Ramp Facility Code
175  0  dest_auto_ramp_facility_code       1      A        777   VARCHAR(1)      NOT NULL  Dest. Automobile Ramp Facility Code
176  0  orig_intermodal_flag               1      A        778   VARCHAR(1)      NOT NULL  Origin Intermodal Flag
177  0  dest_intermodal_flag               1      A        779   VARCHAR(1)      NOT NULL  Destination Intermodal Flag
193  0  transborder_flag                   1      N        780   INT             NOT NULL  Transborder Flag
194  0  orig_rr_country                    2      A    781-782   VARCHAR(2)      NOT NULL  Origin Railroad Country Code
195  0  intrchng_rr_country_1              2      A    783-784   VARCHAR(2)          NULL  First Interchange Railroad Country Code
196  0  intrchng_rr_country_2              2      A    785-786   VARCHAR(2)          NULL  Second Interchange Railroad Country Code
197  0  intrchng_rr_country_3              2      A    787-788   VARCHAR(2)          NULL  Third Interchange Railroad Country Code
198  0  intrchng_rr_country_4              2      A    789-790   VARCHAR(2)          NULL  Fourth Interchange Railroad Country Code
199  0  intrchng_rr_country_5              2      A    791-792   VARCHAR(2)          NULL  Fifth Interchange Railroad Country Code
200  0  intrchng_rr_country_6              2      A    793-794   VARCHAR(2)          NULL  Sixth Interchange Railroad Country Code
201  0  term_rr_country                    2      A    795-796   VARCHAR(2)      NOT NULL  Termination Railroad Country Code
202  0  fuel_surcharge                     9      N    797-805   INT             NOT NULL  Fuel Surcharge
179  0  blank                             13    A/N    806-818   VARCHAR(8)          NULL  Blank (Space reserved for future changes)
180  0  orig_census_region                 4      A    819-822   VARCHAR(4)          NULL  Origin Census Region
181  0  term_census_region                 4      A    823-826   VARCHAR(4)          NULL  Termination Census Region
182  0  exact_expansion_factor             7      N    827-833   FLOAT           NOT NULL  Exact Expansion Factor
183  0  total_variable_cost                8      N    834-841   INT             NOT NULL  Total Variable Cost
185  0  rr_1_variable_cost                 8      N    842-849   INT             NOT NULL  Railroad 1 Variable Cost
186  0  rr_2_variable_cost                 8      N    850-857   INT             NOT NULL  Railroad 2 Variable Cost
187  0  rr_3_variable_cost                 8      N    858-865   INT             NOT NULL  Railroad 3 Variable Cost
188  0  rr_4_variable_cost                 7      N    866-872   INT             NOT NULL  Railroad 4 Variable Cost
189  0  rr_5_variable_cost                 7      N    873-879   INT             NOT NULL  Railroad 5 Variable Cost
190  0  rr_6_variable_cost                 7      N    880-886   INT             NOT NULL  Railroad 6 Variable Cost
191  0  rr_7_variable_cost                 7      N    887-893   INT             NOT NULL  Railroad 7 Variable Cost
192  0  rr_8_variable_cost                 7      N    894-900   INT             NOT NULL  Railroad 8 Variable Cost
"""


# ==================================================================================================
# WAYBILL FIELD OBJECT
# ==================================================================================================

class WaybillField:

    # ----------------------------------------------------------------------------------------------
    def __init__(self, field_num, key_field, field_name, num_positions, field_type, fr_to_cols,
            sql_type, sql_nullable, desciption
            ):

        self.field_num = int(field_num)

        self.key_field = False
        if int(key_field) == 1:
            self.key_field = True

        self.field_name = field_name
        self.num_positions = int(num_positions)
        self.field_type = field_type

        if '-' in fr_to_cols:
            tmp = fr_to_cols.split('-')
            self.fr_to_cols = (int(tmp[0]), int(tmp[1]))
        else:
            self.fr_to_cols = (int(fr_to_cols),)


        self.sql_type = sql_type
        self.sql_nullable = sql_nullable
        self.desciption = desciption


    # ----------------------------------------------------------------------------------------------
    def asText(self):

        return '{}, {}, {}, {}, {}, {}, {}, {}, {}'.format(
                self.field_num, self.key_field, self.field_name, self.num_positions,
                self.field_type, self.fr_to_cols, self.sql_type, self.sql_nullable,
                self.desciption
                )


# ==================================================================================================
# waybill field specs is meant to be readable, take the readable version
# and make it into a list of WaybillField objects
# ==================================================================================================

def make_list_from_waybill_field_specs():

    waybill_fields_list = []

    lines = WAYBILL_FIELD_SPECS.split('\n')

    for x in lines:

        if len(x) > 0:

            field_num       = x[0:3].strip()
            key_field       = x[5].strip()
            field_name      = x[8:40].strip()
            num_positions   = x[42:46].strip()
            field_type      = x[48:51].strip()
            fr_to_cols      = x[55:63].strip()
            sql_type        = x[65:79].strip()
            sql_nullable    = x[81:90].strip()
            desciption      = x[91:].strip()

            field = WaybillField(field_num, key_field, field_name, num_positions, field_type,
                    fr_to_cols, sql_type, sql_nullable, desciption
                    )

            waybill_fields_list.append(field)

    return waybill_fields_list

# ==================================================================================================
# CONVERT FROM FIXED WIDTH TO TAB DELIMITED
# ==================================================================================================

# TODO - check for existence of input file
# TODO - check input file for tabs
# TODO - report status (num of records processed)


def convert_from_fixed_width_to_tab_delimited(input_file, output_file, subset_fields):

    logging.info('Converting fixed width waybill data to tab delimited format ...')

    waybill_field_specs = make_list_from_waybill_field_specs()

    # internal debug
    #for x in waybill_field_specs:
    #   print x.asText()

    unpacker = wbutl.build_record_unpacker(waybill_field_specs, subset_fields)

    with open(output_file, 'w') as wf:

        # WRITE THE HEADER
        header_string = ''
        for field_spec in waybill_field_specs:
            if subset_fields:
                if field_spec.key_field:
                    header_string += field_spec.field_name + '\t'
            else:
                header_string += field_spec.field_name + '\t'

        wf.write(header_string.rstrip('\t') + '\n')

        # READ THE FIXED FORMAT DATA AND WRITE THE TAB DELIMITED RECORDS
        lines_written = 0
        with open(input_file, 'r') as rf:
            for line in rf:
                raw_fields = unpacker(line)
                stripped_raw_fields = [x.strip() for x in raw_fields]
 
                wf.write('\t'.join(stripped_raw_fields) + '\n')
                lines_written += 1

    logging.info('Converted {:,} fixed width waybill data records'.format(lines_written))


# ==================================================================================================
# ENTRY POINT
# ==================================================================================================

if __name__ == "__main__":

    program_name = os.path.basename(__file__)

    if len(sys.argv) != 2:
        print('usage: ' + program_name + ' <config_file>')
        sys.exit()

    full_path_to_config_file = sys.argv[1]

    if not os.path.exists(full_path_to_config_file):
        print('ERROR: config file {} can''t be found!'.format(full_path_to_config_file))
        sys.exit()

    cfg = wbutl.read_config_file(full_path_to_config_file)

    if not os.path.exists(cfg['RUN_DIRECTORY']):
        print('ERROR: run directory {} can''t be found!'.format(cfg['RUN_DIRECTORY']))
        sys.exit()

    output_dir = os.path.join(cfg['RUN_DIRECTORY'], 'output')

    if not os.path.exists(output_dir):
        print('ERROR: output directory {} can''t be found!'.format(output_dir))
        sys.exit()

    wbutl.setup_logging(
            cfg['RUN_DIRECTORY'],
            program_name,
            cfg['FILE_LOGGING_LEVEL'],
            cfg['CONSOLE_LOGGING_LEVEL']
            )

    # debug print config file params
    for k, v in cfg.iteritems():
        logging.debug('cfg file param {} = {}'.format(k, v))

    start_time = datetime.datetime.now()

    logging.info("Start time {}".format(start_time.strftime("%Y-%m-%d %H:%M:%S")))

    output_file = os.path.join(output_dir, 'waybill_full_reformatted.txt')

    # cfg['SUBSET_WAYBILL_FIELDS']  # not implemented, hard coded below, is it even needed
    convert_from_fixed_width_to_tab_delimited(
            cfg['RAW_WAYBILL_DATA'],
            output_file,
            False
            )

    connection_string = wbutl.make_connection_string(
            cfg['DB_DRIVER'],
            cfg['DB_SERVER'],
            cfg['DB_NAME'],
            cfg['DB_USER'],
            cfg['DB_PASS'],
            cfg['DB_TRUSTED']
            )


    waybill_field_specs = make_list_from_waybill_field_specs()

    waybill_table_name = 'waybill_full'  # hard coded convention

    create_table_sql = wbutl.make_create_table_sql(waybill_field_specs, waybill_table_name, '')

    wbutl.create_table(connection_string, waybill_table_name, create_table_sql)

    ## NOTE: for bulk insert to work input data must be on same computer as db server
    wbutl.bulk_insert_text_file_to_db(connection_string, waybill_table_name, output_file)


    # APPLY DIRECT OVERRIDES TO RAW WAYBILL DATA THAT HAS BEEN LOADED TO SQL
    # ----------------------------------------------------------------------------------------------

    logging.info("Applying direct overrides to raw waybill data")

    connection = pypyodbc.connect(connection_string)
    cursor = connection.cursor()

    logging.info("----- Changing Carrier Abbreviations -----")
    rr_remaps = []
    rr_remaps.append(('CPUS', 'CPRS'))
    rr_remaps.append(('CNUS', 'CN'))
    rr_remaps.append(('KCSM', 'KCS'))

    rr_flds = ['orig_rr_alpha', 'intrchng_rr_1_alpha',  'intrchng_rr_2_alpha', 'intrchng_rr_3_alpha',
            'intrchng_rr_4_alpha', 'intrchng_rr_5_alpha', 'intrchng_rr_6_alpha', 'term_rr_alpha']

    for rr_fld in rr_flds:

        for rr_remap in rr_remaps:

            # for pretty printing
            if len(rr_remap[1]) == 2:
                sql = "update {} set {:<19} = '{}'   where {:<19} = '{}'".format(
                        waybill_table_name, rr_fld, rr_remap[1], rr_fld, rr_remap[0])
            elif len(rr_remap[1]) == 3:
                sql = "update {} set {:<19} = '{}'  where {:<19} = '{}'".format(
                        waybill_table_name, rr_fld, rr_remap[1], rr_fld, rr_remap[0])
            elif len(rr_remap[1]) == 4:
                sql = "update {} set {:<19} = '{}' where {:<19} = '{}'".format(
                        waybill_table_name, rr_fld, rr_remap[1], rr_fld, rr_remap[0])

            rows_updated = cursor.execute(sql).rowcount

            if rows_updated > 0:
                logging.info('  {:>6,} rows updated by sql: {}'.format(rows_updated, sql))

            cursor.commit()


    #logging.info("----- Changing PTL, SK to NPTL, SK -----")
    #for i in range(1,8):
        #sql = "update {} set intrchng_{}_rule_260 = 'NPTL' where intrchng_{}_rule_260 = 'PTL' and \
                #junc_state_{} = 'SK'".format(
                #waybill_table_name, i, i, i)
        #rows_updated = cursor.execute(sql).rowcount
        #logging.info('  {:>6,} rows updated by sql: {}'.format(rows_updated, sql))
        #cursor.commit()


    connection.close()

    # WRAP UP
    # ----------------------------------------------------------------------------------------------

    wbutl.report_runtime(start_time)

    print('\n') # blank line after run is done
