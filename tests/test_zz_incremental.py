"""O'sib boruvchi yuklash: ikkinchi ishga tushirish API'dan faqat yangi to'lovlarni so'raydi.
(Alohida faylda — boshqa modullardagi o'qish ulanishlari yopilgandan keyin ishlaydi.)"""
import duckdb


def test_incremental_run_fetches_nothing_new(built, settings):
    from pipeline.run import run_pipeline
    run2 = run_pipeline(settings, full_refresh=False, start_api=True, build_report=False)
    con = duckdb.connect(str(settings.paths.warehouse), read_only=True)
    try:
        mode, = con.execute("SELECT mode FROM meta.etl_runs WHERE run_id = ?", [run2]).fetchone()
        rows, = con.execute("SELECT rows FROM meta.source_lineage WHERE run_id = ? AND source = 'treasury_payments'",
                            [run2]).fetchone()
        n_pay, = con.execute("SELECT COUNT(*) FROM dwh.fact_payment").fetchone()
    finally:
        con.close()
    assert mode == "incremental" and rows == 0 and n_pay > 0
