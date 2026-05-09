import { chromium } from "playwright";
import fs from "node:fs/promises";
import path from "node:path";

const APP_URL = process.env.IRIP_FRONTEND_URL || "http://localhost:5173";
const OUT_DIR = path.resolve(process.cwd(), "ui-audit-output");

const tabs = ["Overview", "Insights", "Sentiment", "Benchmark", "Market", "Trust", "Report"];

console.log("Starting IRIP UI audit...");
console.log("Frontend URL:", APP_URL);
console.log("Output folder:", OUT_DIR);

await fs.mkdir(OUT_DIR, { recursive: true });

const browser = await chromium.launch({ headless: true });
const page = await browser.newPage({
  viewport: { width: 1440, height: 820 },
  deviceScaleFactor: 1,
});

const consoleMessages = [];
const failedRequests = [];
const pageErrors = [];

page.on("console", (message) => {
  consoleMessages.push({
    type: message.type(),
    text: message.text(),
  });
});

page.on("pageerror", (error) => {
  pageErrors.push({
    message: error.message,
    stack: error.stack,
  });
});

page.on("requestfailed", (request) => {
  failedRequests.push({
    url: request.url(),
    failure: request.failure()?.errorText,
  });
});

function safeFileName(value) {
  return value.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "");
}

try {
  console.log("Opening app...");
  await page.goto(APP_URL, { waitUntil: "domcontentloaded", timeout: 30000 });

  console.log("Waiting for app to settle...");
  await page.waitForTimeout(2500);

  await page.screenshot({
    path: path.join(OUT_DIR, "00-initial.png"),
    fullPage: false,
  });

  async function auditDom(tabName) {
    return page.evaluate((tabName) => {
      const viewport = {
        width: window.innerWidth,
        height: window.innerHeight,
        bodyScrollWidth: document.body.scrollWidth,
        bodyScrollHeight: document.body.scrollHeight,
        documentScrollWidth: document.documentElement.scrollWidth,
        documentScrollHeight: document.documentElement.scrollHeight,
      };

      const allElements = Array.from(document.querySelectorAll("*"));

      const elementAudits = allElements
        .map((el, index) => {
          const rect = el.getBoundingClientRect();
          const style = window.getComputedStyle(el);
          const text = el.textContent?.replace(/\s+/g, " ").trim() || "";

          const isVisible =
            style.display !== "none" &&
            style.visibility !== "hidden" &&
            Number(style.opacity || "1") > 0 &&
            rect.width > 0 &&
            rect.height > 0;

          if (!isVisible) return null;

          const className =
            typeof el.className === "string"
              ? el.className
              : el.getAttribute("class") || "";

          const selectorHint = [
            el.tagName.toLowerCase(),
            el.id ? `#${el.id}` : "",
            className
              ? `.${className
                  .split(/\s+/)
                  .filter(Boolean)
                  .slice(0, 4)
                  .join(".")}`
              : "",
          ].join("");

          const hasHorizontalOverflow = el.scrollWidth > el.clientWidth + 2;
          const hasVerticalOverflow = el.scrollHeight > el.clientHeight + 2;

          const outsideViewport =
            rect.left < -2 ||
            rect.top < -2 ||
            rect.right > window.innerWidth + 2 ||
            rect.bottom > window.innerHeight + 2;

          const tinyButHasText = text.length > 20 && (rect.width < 16 || rect.height < 10);

          const problem =
            outsideViewport ||
            hasHorizontalOverflow ||
            hasVerticalOverflow ||
            tinyButHasText;

          return {
            index,
            tag: el.tagName.toLowerCase(),
            selectorHint,
            role: el.getAttribute("role"),
            ariaLabel: el.getAttribute("aria-label"),
            text: text.slice(0, 220),
            rect: {
              x: Math.round(rect.x),
              y: Math.round(rect.y),
              width: Math.round(rect.width),
              height: Math.round(rect.height),
              bottom: Math.round(rect.bottom),
              right: Math.round(rect.right),
            },
            overflow: {
              overflowX: style.overflowX,
              overflowY: style.overflowY,
              scrollWidth: el.scrollWidth,
              clientWidth: el.clientWidth,
              scrollHeight: el.scrollHeight,
              clientHeight: el.clientHeight,
              hasHorizontalOverflow,
              hasVerticalOverflow,
            },
            outsideViewport,
            tinyButHasText,
            problem,
          };
        })
        .filter(Boolean);

      const problemElements = elementAudits.filter((item) => item.problem);

      const importantSelectors = [
        ".irip-app-shell",
        ".top-import-bar",
        ".irip-body-grid",
        ".irip-left-workspace",
        ".irip-main-tile",
        ".main-visual-content",
        ".right-control-panel",
        ".overview-view",
        ".summary-view",
        ".split-chart-view",
        ".competitor-view",
        ".news-view",
        ".quality-view",
        ".report-view",
        ".echart-card",
        ".echart-stage",
        ".workflow-tile",
        ".kpi-card",
        ".gap-note-card",
        ".signal-chip-zone",
        ".summary-panel",
        ".report-preview-card",
        ".trust-panel",
      ];

      const importantBoxes = importantSelectors.flatMap((selector) =>
        Array.from(document.querySelectorAll(selector)).map((el, index) => {
          const rect = el.getBoundingClientRect();
          const style = window.getComputedStyle(el);

          return {
            selector,
            index,
            text: el.textContent?.replace(/\s+/g, " ").trim().slice(0, 220),
            rect: {
              x: Math.round(rect.x),
              y: Math.round(rect.y),
              width: Math.round(rect.width),
              height: Math.round(rect.height),
              bottom: Math.round(rect.bottom),
              right: Math.round(rect.right),
            },
            overflow: {
              overflowX: style.overflowX,
              overflowY: style.overflowY,
              scrollWidth: el.scrollWidth,
              clientWidth: el.clientWidth,
              scrollHeight: el.scrollHeight,
              clientHeight: el.clientHeight,
              hasHorizontalOverflow: el.scrollWidth > el.clientWidth + 2,
              hasVerticalOverflow: el.scrollHeight > el.clientHeight + 2,
            },
          };
        })
      );

      const importantProblems = importantBoxes.filter(
        (box) =>
          box.rect.width <= 1 ||
          box.rect.height <= 1 ||
          box.rect.bottom > window.innerHeight + 2 ||
          box.rect.right > window.innerWidth + 2 ||
          box.overflow.hasHorizontalOverflow ||
          box.overflow.hasVerticalOverflow
      );

      const echarts = Array.from(document.querySelectorAll(".echart-stage")).map((el, index) => {
        const rect = el.getBoundingClientRect();
        return {
          index,
          width: Math.round(rect.width),
          height: Math.round(rect.height),
          zeroSized: rect.width <= 1 || rect.height <= 1,
        };
      });

      return {
        tabName,
        viewport,
        hasPageScroll:
          document.documentElement.scrollHeight > window.innerHeight + 2 ||
          document.documentElement.scrollWidth > window.innerWidth + 2,
        totalVisibleElements: elementAudits.length,
        totalProblemElements: problemElements.length,
        problemElements: problemElements.slice(0, 120),
        importantBoxes,
        importantProblems,
        echarts,
      };
    }, tabName);
  }

  const audits = [];

  for (const tab of tabs) {
    console.log("Auditing tab:", tab);

    const button = page.locator(".view-chip").filter({ hasText: tab }).first();

    try {
      await button.click({ timeout: 7000 });
    } catch (error) {
      console.log(`Could not click tab "${tab}". Saving failure screenshot.`);
      await page.screenshot({
        path: path.join(OUT_DIR, `failed-${safeFileName(tab)}.png`),
        fullPage: false,
      });

      audits.push({
        tabName: tab,
        error: `Could not click tab: ${error.message}`,
      });

      continue;
    }

    await page.waitForTimeout(1000);

    const fileName = `${tabs.indexOf(tab) + 1}-${safeFileName(tab)}.png`;

    await page.screenshot({
      path: path.join(OUT_DIR, fileName),
      fullPage: false,
    });

    audits.push(await auditDom(tab));
  }

  const report = {
    appUrl: APP_URL,
    generatedAt: new Date().toISOString(),
    consoleMessages,
    failedRequests,
    pageErrors,
    audits,
  };

  await fs.writeFile(
    path.join(OUT_DIR, "audit-report.json"),
    JSON.stringify(report, null, 2)
  );

  console.log("UI audit complete.");
  console.log("Files created in:", OUT_DIR);
} catch (error) {
  console.error("UI audit failed:", error);

  await fs.writeFile(
    path.join(OUT_DIR, "audit-error.txt"),
    String(error?.stack || error?.message || error)
  );
} finally {
  await browser.close();
}