const playwright = require("playwright");
const fs = require("fs");
const { default: PQueue } = require("p-queue");
const { get } = require("http");
const fetch = (...args) =>
  import("node-fetch").then((mod) => mod.default(...args));
const FormData = require("form-data");
const csv = require("csv-parser");

const CONCURRENCY = 10;
const TIMEOUT = 30000;
const MAX_RETRIES = 20;

const dump =
  "/Users/user/SynologyDrive/Beruf/Projekte/farmsubsidies/dumps/it_2023/Trasparenza2023_Ita.csv";

async function readCsv(dump) {
  return new Promise((resolve, reject) => {
    const results = [];
    fs.createReadStream(dump)
      .pipe(csv({ separator: ";" }))
      .on("data", (data) => results.push(data))
      .on("end", () => resolve(results))
      .on("error", reject);
  });
}

const scrapeTable = async (page, code, location) => {
  const table = await page.locator("table#results");
  const tableHtml = await table.innerHTML();
  if (tableHtml.length > 1) {
    fs.writeFileSync(`data/it/${location}-${code}.html`, tableHtml);
    console.log(`Page ${code} ${location} saved to file.`);
    return true;
  }
  return false;
};

// // generate strings from 01 to 99
// const generateAllCodes = () => {
//   const codes = [];
//   for (let i = 10; i <= 99; i++) {
//     const code = i.toString().padStart(2, "0");
//     codes.push(code);
//   }
//   return codes;
// };

const solveCaptcha = async (page) => {
  // Find the captcha image and get its src
  const captchaImg = await page.locator("img[src*='Captcha.jpg']");
  const imgSrc = await captchaImg.getAttribute("src");
  // console.log("Captcha image source:", imgSrc);

  // Take a screenshot of the captcha image as a buffer
  const captchaBuffer = await captchaImg.screenshot();

  // Use Node.js FormData
  const formData = new FormData();
  formData.append("file", captchaBuffer, {
    filename: "captcha.jpg",
    contentType: "image/jpeg",
  });

  // Send the image to the server for processing
  const response = await fetch("http://127.0.0.1:5001/predict", {
    method: "POST",
    body: formData,
    headers: formData.getHeaders(),
  });

  const result = await response.json();
  console.log("Captcha result:", result);

  return result.result; // or handle error/result as needed
};

const getResult = async (page, location, code) => {
  // enter location, input with name 'ricercaComuneResidenza'
  await page.locator("input[name='ricercaComuneResidenza']").fill(location);
  await page.waitForTimeout(1000);

  if (code.includes("-")) {
    // split code by '-'
    const codeParts = code.split("-");
    // enter code, input with name 'ricercaMacroMisura'
    await page
      .locator("select[name='ricercaMacroMisura']")
      .selectOption(codeParts[0]);
    await page.waitForLoadState("networkidle");
    await page.waitForTimeout(5000);

    // get all options from select with name 'ricercaMisura'
    const options = await page
      .locator("select[name='ricercaMisura'] option")
      .all();
    const idx = parseInt(codeParts[1]);
    if (!options[idx]) {
      throw new Error(
        `Option index ${idx} not found in 'ricercaMisura' select`
      );
    }
    const val = await options[idx].getAttribute("value");
    await page.locator("select[name='ricercaMisura']").selectOption(val);

    if (codeParts.length > 2) {
      await page
        .locator("select[name='ricercaImportoPagamenti']")
        .selectOption(codeParts[2]);
    }
  } else {
    await page.locator("select[name='ricercaMacroMisura']").selectOption(code);
    await page.waitForLoadState("networkidle");
    await page.waitForTimeout(5000);
  }

  const captchaResult = await solveCaptcha(page);

  await page.locator("input[name='caratteriImmagine']").fill(captchaResult);

  // click on the button with the text "Find"
  await page.locator("input:has-text('Find')").first().click();

  await page.waitForLoadState("networkidle");
  await page.waitForTimeout(5000);

  // The characters you type do not match those in the image.

  const errorMessage = await page
    .locator("img:has-text('do not match')")
    .isVisible();

  if (errorMessage) {
    console.log("Captcha error, retrying...");
    return getResult(page, location, code);
  }

  //  No beneficiary found for the chosen criteria.
  const noResults = await page
    .locator("text=No beneficiary found for the chosen criteria.")
    .isVisible();

  if (noResults) {
    fs.writeFileSync(`data/it/${location}-${code}.html`, "No results");
    console.log("No results found for code", code, location);
    return;
  }

  const isSuccess = await scrapeTable(page, code, location);

  if (isSuccess) {
    console.log("Success! Found results for code", code, location);
  }

  if (!isSuccess) {
    console.log("No results found for code", code, location);
    throw new Error("No results found");
  }
  return;
};

const oneRun = async (browser, location, code) => {
  let tryCount = 0;
  while (tryCount < MAX_RETRIES) {
    try {
      console.log(
        `Starting run for code ${location} ${code}, try ${tryCount + 1}`
      );
      // skip if html file exists
      if (fs.existsSync(`data/it/${location}-${code}.html`)) {
        console.log(`Code ${code} already exists, skipping.`);
        return;
      }
      if (CONCURRENCY > 1) {
        const randomWait = Math.floor(Math.random() * TIMEOUT);
        await new Promise((resolve) => setTimeout(resolve, randomWait));
      }
      const context = await browser.newContext({ ignoreHTTPSErrors: true }); // Bypass SSL errors
      context.setDefaultTimeout(
        tryCount > MAX_RETRIES / 2 ? TIMEOUT * 2 : TIMEOUT
      );
      const page = await context.newPage();

      await page.goto(
        "https://www.agea.gov.it/portale-agea/servizi/pubblicazione-dei-beneficiari"
      );
      await page.waitForLoadState("networkidle");
      // await page.waitForTimeout(1000);

      // click on first button with the text "Vai al servizio"
      await page
        .locator("button span:has-text('Vai al servizio')")
        .first()
        .click();

      await page.waitForLoadState("networkidle");
      await page.waitForTimeout(2000);

      // close old tab
      const pages = context.pages();
      // console.log("Pages length:", pages.length);
      if (pages.length > 1) {
        // focus on new tab
        await pages[1].bringToFront();
        // console.log("Focused on new tab");
        // close old tab
        await pages[0].close();
        // console.log("Closed old tab");
      }

      const newPage = context.pages()[0];

      // click on the link with the title "English"
      await newPage.waitForTimeout(5000);
      await newPage.locator("img[title='English']").first().click();
      await newPage.waitForLoadState("networkidle");
      await newPage.waitForTimeout(5000);
      // click on the link with the text "Search for beneficiaries"

      await newPage.locator("a:has-text('BENEFICIARIES')").first().click();
      await newPage.waitForLoadState("networkidle");
      // await newPage.waitForTimeout(2000);

      await getResult(newPage, location, code);

      console.log(`Finished ${code}.`);
      await newPage.close();
      break; // Success, exit retry loop
    } catch (error) {
      tryCount++;
      console.error(`Error in code ${code} (try ${tryCount}):`, error);
      if (tryCount >= MAX_RETRIES) {
        console.error(
          `Failed after ${MAX_RETRIES} attempts for code ${code}, location ${location}`
        );
        // append location and code to a file
        fs.appendFileSync("failed.txt", `${location}-${code}\n`);
      } else {
        console.log(
          `Retrying code ${code}, location ${location} (attempt ${
            tryCount + 1
          })...`
        );
      }
    }
  }
};

(async () => {
  const launchOptions = {
    headless: true,
  };

  const browser = await playwright.chromium.launch(launchOptions);

  const queue = new PQueue({ concurrency: CONCURRENCY });
  // const allCodes = generateAllCodes();
  let allCodes = [
    "1",
    "2-1",
    "2-2",
    "2-3",
    "2-4",
    "2-5",
    "2-6-1",
    "2-6-2",
    "2-6-3",
    "2-6-4",
    "2-6-5",
    "2-6-6",
    "2-7-1",
    "2-7-2",
    "2-7-3",
    "2-7-4",
    "2-7-5",
    "2-7-6",
    "2-8-1",
    "2-8-2",
    "2-8-3",
    "2-8-4",
    "2-8-5",
    "2-8-6",
    "2-9",
    "2-10",
    "3",
    "4",
    "5",
    "6",
    "7",
    "8",
    "9",
  ];

  for (let j = 1; j < 7; j++) {
    for (let i = 1; i < 25; i++) {
      allCodes.push("4-" + i + "-" + j);
    }
    // for (let i = 1; i < 25; i++) {
    allCodes.push("2-" + "5" + "-" + j);
    allCodes.push("2-" + "9" + "-" + j);
    allCodes.push("2-" + "10" + "-" + j);
    // }
  }
  // allCodes = [...allCodes, ...allCodes, ...allCodes];
  // allCodes = [...allCodes, ...allCodes, ...allCodes];

  const results = await readCsv(dump);
  const allLocation = results.map((row) => row["Comune"]);

  // get counts of all locations
  const locationCounts = {};
  allLocation.forEach((location) => {
    if (locationCounts[location]) {
      locationCounts[location]++;
    } else {
      locationCounts[location] = 1;
    }
  });
  // sort locations by count
  const sortedLocations = Object.entries(locationCounts).sort(
    (a, b) => b[1] - a[1]
  );
  console.log("Sorted locations:", sortedLocations.slice(0, 10));

  // displat locations with onle one entry
  const singleEntryLocations = Object.entries(locationCounts).filter(
    (entry) => entry[1] == 1
  );
  // console.log("Locations with only one entry:", singleEntryLocations);

  // only keep locations with more than 10 entries
  const filteredLocations = Object.entries(locationCounts)
    .filter((entry) => entry[1] > 1)
    .filter((entry) => entry[0] != "ROMA")
    .filter((entry) => entry[0] != "SISSA TRECASALI")
    .filter((entry) => entry[0] != "BADIA .ABTEI.")
    .filter((entry) => entry[0] != "APPIANO SULLA STRADA DEL VIN")
    .filter((entry) => entry[0] != "MONTEBELLO JONICO")
    .filter((entry) => entry[0] != "RENON .RITTEN.") // buggy location
    .sort((a, b) => b[1] - a[1]);
  // console.log("Locations with more than 10 entries:", filteredLocations);

  const uniqueLocations = filteredLocations.map((entry) => entry[0]);
  // const uniqueLocations = [...new Set(allLocation)];

  console.log("Unique locations:", uniqueLocations.length);

  const allPromises = [];

  const unfinished = allCodes
    .filter((code) => {
      return (
        !fs.existsSync(`data/it/${""}-${code}.html`) &&
        code.split("-").length == 3
      );
    })
    .map((code) => {
      // if (code[0] == "2" && "678".includes(code.split("-")[1])) return code;
      if (code[0] == "2") return code[0] + "-" + code.split("-")[1];

      return code.split("-")[0];
    });

  const setUnfinished = new Set(unfinished);
  const unfinishedArray = [...setUnfinished];

  for (const location of uniqueLocations) {
    if (!location) continue;
    // for (const location of uniqueLocations) {
    // for (const code of allCodes) {
    //   if (code.split("-").length == 3) {
    //     allPromises.push(queue.add(() => oneRun(browser, location, code)));
    //   }
    // }
    for (const code of unfinishedArray) {
      if (["NOCI", "DELICETO"].includes(location) && code.startsWith("2-7")) {
        allPromises.push(queue.add(() => oneRun(browser, location, "2-7-1")));
        allPromises.push(queue.add(() => oneRun(browser, location, "2-7-2")));
        allPromises.push(queue.add(() => oneRun(browser, location, "2-7-3")));
        allPromises.push(queue.add(() => oneRun(browser, location, "2-7-4")));
        allPromises.push(queue.add(() => oneRun(browser, location, "2-7-5")));
        allPromises.push(queue.add(() => oneRun(browser, location, "2-7-6")));
      } else {
        allPromises.push(queue.add(() => oneRun(browser, location, code)));
      }
    }
  }

  // const allPromises = allCodes.map((code) => {
  //   return queue.add(() => oneRun(browser, "Roma", code));
  // });
  await Promise.all(allPromises);
  console.log("All pages scraped.");

  await browser.close();
})();
