const playwright = require("playwright");
const fs = require("fs");
const { default: PQueue } = require("p-queue");

const scrapeTable = async (page, code) => {
  const table = await page.locator("table");
  const tableHtml = await table.innerHTML();
  if (tableHtml.length > 1) {
    // check if pagination is present
    const pagination = await page
      .locator(
        "div.ui-paginator.ui-paginator-bottom.ui-widget-header.ui-corner-bottom"
      )
      .isVisible();
    let pageNumber = 0;
    if (pagination) {
      pageNumber = await page
        .locator(
          "div.ui-paginator.ui-paginator-bottom.ui-widget-header.ui-corner-bottom a.ui-state-active"
        )
        .innerText();
    }
    // write the tableHtml to a file
    fs.writeFileSync(`data/nl/${code}-${pageNumber}.html`, tableHtml);
    console.log(`Page ${code} ${pageNumber} saved to file.`);
    return pageNumber;
  }
  throw new Error("Table is empty");
};

// generate strings from 01 to 99
const generateAllCodes = () => {
  const codes = [];
  for (let i = 10; i <= 99; i++) {
    const code = i.toString().padStart(2, "0");
    codes.push(code);
  }
  return codes;
};

const oneRun = async (browser, code) => {
  try {
    console.log(`Starting run for code ${code}`);
    if (fs.existsSync(`data/nl/${code}.txt`)) {
      console.log(`Code ${code} already exists, skipping.`);
      return;
    }

    const context = await browser.newContext({ ignoreHTTPSErrors: true }); // Bypass SSL errors
    const page = await context.newPage();

    await page.goto("https://mijn.rvo.nl/europese-subsidies-2022");
    await page.waitForLoadState("networkidle");
    await page.waitForTimeout(1000);

    await page.waitForSelector(
      "#_EuSubsidies_WAR_EuSubsidiesportlet_\\:subsidiesForm\\:subsidie_label"
    );
    await page
      .locator(
        "#_EuSubsidies_WAR_EuSubsidiesportlet_\\:subsidiesForm\\:subsidie_label"
      )
      .click();
    await page.waitForTimeout(1000);
    // await page.waitForSelector("text=Gemeenschappelijk Landbouw Beleid", {
    //   timeout: 10000,
    // });

    // await page.locator("text=Gemeenschappelijk Landbouw Beleid¿").click();
    await page
      .locator(
        "#_EuSubsidies_WAR_EuSubsidiesportlet_\\:subsidiesForm\\:subsidie_1"
      )
      .click();
    await page.waitForTimeout(2000);

    await page.waitForSelector(
      "#_EuSubsidies_WAR_EuSubsidiesportlet_\\:subsidiesForm\\:postcode"
    );

    // input the code
    await page
      .locator(
        "#_EuSubsidies_WAR_EuSubsidiesportlet_\\:subsidiesForm\\:postcode"
      )
      .fill(code);

    await page.waitForTimeout(5000);
    await page.locator('span.ui-button-text:text("Zoek")').click();

    await page.waitForLoadState("networkidle");
    await page.waitForTimeout(5000);

    if (
      await page
        .locator(
          "text=Er zijn geen resultaten gevonden die voldoen aan uw zoekcriteria."
        )
        .isVisible()
    ) {
      console.log(`No results found for code ${code}`);
      await page.close();
      return;
    }

    const choice = await page.locator(
      'div.ui-paginator-top select[title="Rijen per Pagina"]'
    );
    if (await choice.isVisible()) {
      await choice.selectOption("100");
    }

    await page.waitForTimeout(1000);

    let prevPageNumber = -1;
    while (true) {
      while (true) {
        const pageNumber = await scrapeTable(page, code);
        if (pageNumber !== prevPageNumber) {
          prevPageNumber = pageNumber;
          break;
        }
        console.log("Page number is the same, retrying...");
        await page.waitForTimeout(2000);
      }

      if (prevPageNumber == 0) {
        console.log("No more pages to scrape, exiting loop.");
        break;
      }

      // 101-126 van 126 resultaten, just an example!
      const resultText = await page
        .locator(
          "div.ui-paginator.ui-paginator-bottom.ui-widget-header.ui-corner-bottom span.ui-paginator-current"
        )
        .innerText();

      const resultNumber = parseInt(resultText.split("van")[0].split("-")[1]);
      // check if the result is the final one
      if (resultText.includes(" " + resultNumber + " ")) {
        console.log("Last page reached, exiting loop.");
        break;
      }

      await page
        .locator(
          '[id="_EuSubsidies_WAR_EuSubsidiesportlet_\\:subsidiesForm\\:glbTable_paginator_bottom"] span.ui-icon-seek-next'
        )
        .click();
      await page.waitForLoadState("networkidle");

      await page.waitForTimeout(1000);
    }
    // create a file with the code
    fs.writeFileSync(`data/nl/${code}.txt`, code);
    console.log(`Finished ${code}.`);
    await page.close();
  } catch (error) {
    console.error(`Error in code ${code}:`, error);
  }
};

(async () => {
  const launchOptions = {
    headless: false,
  };

  const browser = await playwright.chromium.launch(launchOptions);

  const queue = new PQueue({ concurrency: 10 });
  const allCodes = generateAllCodes();

  const allPromises = allCodes.map((code) => {
    return queue.add(() => oneRun(browser, code));
  });
  await Promise.all(allPromises);
  console.log("All pages scraped.");

  await browser.close();
})();
