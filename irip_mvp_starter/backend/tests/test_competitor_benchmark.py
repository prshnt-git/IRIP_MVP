from app.db.repository import ReviewRepository
from app.pipeline.review_analyzer import ReviewAnalyzer
from app.schemas.review import ReviewInput
from app.services.product_catalog_service import ProductCatalogImportService


def test_catalog_mapping_and_benchmark(tmp_path):
    repo = ReviewRepository(tmp_path / "test.db")
    analyzer = ReviewAnalyzer()
    catalog = ProductCatalogImportService(repo)
    result = catalog.import_csv_text(
        "product_id,product_name,brand,price_band,own_brand,competitor_product_ids\n"
        "phone_a,Demo Phone A,TECNO,10000-15000,true,phone_b\n"
        "phone_b,Competitor Phone B,Other,10000-15000,false,\n"
    )
    assert result.imported_products == 2
    assert result.imported_mappings == 1
    assert repo.list_competitors("phone_a")[0]["product_id"] == "phone_b"

    review_a = ReviewInput(
        review_id="a1",
        product_id="phone_a",
        product_name="Demo Phone A",
        source="flipkart",
        rating=3,
        review_date="2026-05-01",
        raw_text="Camera mast hai but battery backup bekar hai",
        verified_purchase=True,
    )
    review_b = ReviewInput(
        review_id="b1",
        product_id="phone_b",
        product_name="Competitor Phone B",
        source="flipkart",
        rating=4,
        review_date="2026-05-01",
        raw_text="Battery backup mast hai but camera bekar hai",
        verified_purchase=True,
    )
    repo.save_review_analysis(review_a, analyzer.analyze(review_a))
    repo.save_review_analysis(review_b, analyzer.analyze(review_b))

    benchmark = repo.get_competitor_benchmark("phone_a", "phone_b")
    assert benchmark["product_id"] == "phone_a"
    assert benchmark["competitor_product_id"] == "phone_b"
    aspects = {item["aspect"]: item for item in benchmark["benchmark_aspects"]}
    assert "battery" in aspects
    assert "camera" in aspects
    assert aspects["camera"]["gap"] > 0
    assert aspects["battery"]["gap"] < 0
