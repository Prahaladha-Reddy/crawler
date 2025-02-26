#fastapi

import json
import asyncio

from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, CacheMode
from crawl4ai.extraction_strategy import JsonCssExtractionStrategy
from fastapi import FastAPI, HTTPException, Query
app = FastAPI(title="Job Listings Scraper API", description="Extracts job listings using crawl4AI.", version="1.0")


async def extract_jobs(job_links, jobs_data):
    # 1. Define schemas for multiple websites
    await asyncio.sleep(1)

    jobs_data = jobs_data
    unique_entries = set()

    async with AsyncWebCrawler(verbose=True) as crawler:
        for link in job_links:  # 🔥 Loop over each URL in the list
            print(f"Extracting from: {link}")
            schema = {
                "name": "Noticebard Internship Listings",
                "baseSelector": "div.backdrop-blur-sm.rounded-xl.border",
                "fields": [
                    {"name": "image_url", "selector": "img.rounded-xl", "type": "attribute", "attribute": "src"},
                    {"name": "title", "selector": "h2.text-xl.font-semibold", "type": "text"},
                    {"name": "apply_link", "selector": "a.bg-blue-600", "type": "attribute", "attribute": "href"}
                ]
            }
            extraction_strategy = JsonCssExtractionStrategy(schema, verbose=True)
            config = CrawlerRunConfig(
                cache_mode=CacheMode.BYPASS,
                extraction_strategy=extraction_strategy,
            )

            result = await crawler.arun(url=link, config=config)  # ✅ Passing a single URL now
            if result.success:
                data = json.loads(result.extracted_content)
                filtered_data = []
                for item in data:
                    if item:
                        identifier = tuple(item.get(key) for key in item)
                        if identifier not in unique_entries:
                            unique_entries.add(identifier)
                            filtered_data.append(item)
                jobs_data.append({"data": filtered_data})
                print(f"Extracted {len(filtered_data)} unique entries from {link}")
            else:
                print(f"Crawl failed for {link}: {result.error_message}")

job_links = ["https://www.talentd.in/all-jobs.php?page="+str(i)+"&job_id=&role=&employment_type=&title=&batch_years=" for i in range(2, 11)]
fresher_links = ["https://www.talentd.in/freshers-jobs.php?page="+str(i)+"&job_type=Fresher&job_id=&role=&employment_type=&title=&batch_years=" for i in range(2, 11)]
intern_links = ["https://www.talentd.in/internship-jobs.php?page="+str(i)+"&job_type=Internship&job_id=&role=&employment_type=&title=&batch_years=" for i in range(2, 11)]

# ✅ Running the async function
# if _name_ == "_main_":
#     nest_asyncio.apply()  # Helps avoid asyncio runtime errors in some environments
#     asyncio.run(extract_jobs(job_links))
#     asyncio.run(extract_jobs(fresher_links))
#     asyncio.run(extract_jobs(intern_links))
#     print("✅ Saved all unique extracted data to all")

@app.get("/extract/all", summary="Extract all job listings")
async def get_all_jobs():
    job_links = [f"https://www.talentd.in/all-jobs.php?page={i}&job_id=&role=&employment_type=&title=&batch_years=" for i in range(2, 10)]
    jobs_data = []
    jobs = await extract_jobs(job_links, jobs_data)
    return {"status": "success", "data": jobs_data}
# print(jobs)

@app.get("/extract/fresher", summary="Extract fresher job listings")
async def get_fresher_jobs():
    fresher_links = [f"https://www.talentd.in/freshers-jobs.php?page={i}&job_type=Fresher&job_id=&role=&employment_type=&title=&batch_years=" for i in range(2, 10)]
    fresher_data = []
    jobs = await extract_jobs(fresher_links, fresher_data)
    return {"status": "success", "data": fresher_data}


@app.get("/extract/intern", summary="Extract internship job listings")
async def get_intern_jobs():
    intern_links = [f"https://www.talentd.in/internship-jobs.php?page={i}&job_type=Internship&job_id=&role=&employment_type=&title=&batch_years=" for i in range(2, 10)]
    intern_data = []
    jobs = await extract_jobs(intern_links, intern_data)
    return {"status": "success", "data": intern_data}


@app.get("/", summary="Health Check Endpoint")
async def root():
    return {"message": "🚀 Job Listings Scraper API is running!"}
# To run the API
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
