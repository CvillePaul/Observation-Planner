from sqlite3 import connect
import datetime as dt
import functools
from astropy.coordinates import SkyCoord
from astropy.table import Table
from astropy.time import Time
import astropy.units as u
import astroplan as ap
import pandas as pd

def pick_targets():
    conn = connect("../django/QuadEB/db.sqlite3")

    observing_lists =[
        "LBT Spectra from pepsi_files_database.txt|Robby|2023-11-22",
        "Featured targets from 2023-11-27 meeting",
        "High Quality Non Detections from Speckle-High-Quality-Non-Detections-Table|Jimmy|2023-11-22",
        "Resolved Binaries from Speckle-Resolved-Table|Jimmy|2023-11-22",
        "brighter than 13",
    ]

    list_members = {}
    for observing_list in observing_lists:
        list_targets = set(pd.read_sql("""
            select t.local_id
                from tom_targetlist tl
                join tom_targetlist_targets tlt on tl.id = tlt.targetlist_id
                join tom_target t on t.id = tlt.target_id
                where tl.name = '""" + observing_list + "';",
            conn)["local_id"])
        list_members[observing_list] = list_targets

    # desired_targets = functools.reduce(set.union, list_members.values())
    desired_targets = list_members["Featured targets from 2023-11-27 meeting"].union(list_members["High Quality Non Detections from Speckle-High-Quality-Non-Detections-Table|Jimmy|2023-11-22"])
    desired_targets = desired_targets.intersection(list_members["brighter than 13"])

    target_table = Table.from_pandas(pd.read_sql(f"select * from tom_target where local_id in ('{"', '".join(desired_targets)}');", conn))

    targets = [
        ap.FixedTarget(
            coord=SkyCoord(
                frame='icrs',
                obstime=Time("2000.0", format="jyear", scale="tdb"),
                ra=ra*u.deg,
                dec=dec*u.deg,
                pm_ra_cosdec=pmra*u.mas/u.yr,
                pm_dec=pmdec*u.mas/u.yr,
            ),
            name=local_id,
        )
        for local_id, ra, dec, pmra, pmdec in target_table[["local_id", "ra", "dec", "pmra", "pmdec"]]
    ]

    observer = ap.Observer.at_site("lbt")

    constraints = [
        ap.AltitudeConstraint(10*u.deg, 80*u.deg),
        ap.AirmassConstraint(1.6),
        # ap.AtNightConstraint.twilight_civil(),
        ]

    observing_nights_utc = [
        "2023-12-06",
        "2023-12-07",
        "2023-12-09",
        "2023-12-10",
        "2023-12-11",
        "2023-12-12",
        ]

    observable_targets, earlies = {}, {}
    for observing_night_utc in observing_nights_utc:
        #TODO: fix shitty handling of time
        early_in_session = Time([f"{observing_night_utc} 00:00", f"{observing_night_utc} 04:00"])
        early_observable = ap.is_observable(constraints, observer, targets, early_in_session)
        rest_of_session = Time([f"{observing_night_utc} 04:00", f"{observing_night_utc} 12:00"])
        later_observable = ap.is_observable(constraints, observer, targets, rest_of_session)
        early_ids = set(target_table[early_observable]["local_id"])
        later_ids = set(target_table[later_observable]["local_id"])
        early_setters = early_ids.difference(later_ids)
        earlies[observing_night_utc] = early_setters
        observable_targets[observing_night_utc] = early_ids | later_ids
        # print(f"{observing_night_utc} {len(early_setters):2d}/{len(later_ids):2d} {", ".join(early_setters)}")
        # print(observing_night_utc)
        # for id in sorted(early_ids | later_ids):
        #     print(id)
    import functools
    common_earlies = functools.reduce(set.intersection, earlies.values())
    # print("earlies", "', '".join(common_earlies))
    common_targets = functools.reduce(set.intersection, observable_targets.values())
    # print("lates", "', '".join(common_targets))
    print("', '".join(common_earlies | common_targets))


if __name__ == "__main__":
    pick_targets()
