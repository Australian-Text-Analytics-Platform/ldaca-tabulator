
# import packages

# main idea
'''
from ldaca.tabulator import LDaCATabulator

CORPUS=""https://data.ldaca.edu.au/api/object/meta?resolve-parts&noUrid&id=arcp%3A%2F%2Fname%2Chdl10.25949~24769173.v1"

tb = new LDaCATabulator(url=CORPUS)

tb.build_tables()

df = tb.get_texts()
print(df.head)

                              entity_id identifier  ...                    author_prov:specializationOf_id author_description
0  arcp://name,hdl10.26180~23961609/item/1-001      1-001  ...  arcp://name,hdl10.26180~23961609/author/Philip...               None
1  arcp://name,hdl10.26180~23961609/item/1-002      1-002  ...  arcp://name,hdl10.26180~23961609/author/Philip...               None
2  arcp://name,hdl10.26180~23961609/item/1-003      1-003  ...  arcp://name,hdl10.26180~23961609/author/Philip...               None
3  arcp://name,hdl10.26180~23961609/item/1-004      1-004  ...  arcp://name,hdl10.26180~23961609/author/Philip...               None
4  arcp://name,hdl10.26180~23961609/item/1-005      1-005  ...  arcp://name,hdl10.26180~23961609/author/Philip...               None

[5 rows x 53 columns]


df2 = tb.get_people()
print(df2.head)

                                           entity_id                                      name  ... homeLocation_id description
0  https://www.peterlang.com/search?f_0=author&q_...                       Clemens W. A. Fritz  ...            None        None
1  arcp://name,hdl10.26180~23961609/author/Philip...  Philip, Arthur - status 1788 text #1-001  ...            None        None
2  arcp://name,hdl10.26180~23961609/author/Philip...                            Philip, Arthur  ...            None        None
3   arcp://name,hdl10.26180~23961609/recipient/1-001                           1-001 Recipient  ...     #place_GB-E        None
4  arcp://name,hdl10.26180~23961609/author/Philip...  Philip, Arthur - status 1788 text #1-002  ...            None        None

[5 rows x 22 columns]


'''

class LDaCATabulator:
    pass