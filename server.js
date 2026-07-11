const express = require('express');
const cors = require('cors');
const { BigQuery } = require('@google-cloud/bigquery');
const { google } = require('googleapis');
const fs = require('fs');

const app = express();
// ใช้ process.env.PORT เพื่อให้รองรับระบบอัตโนมัติของ Render ได้ดีขึ้น
const port = process.env.PORT || 3000;

app.use(cors());
app.use(express.json({ limit: '100mb', type: ['application/json', 'text/plain'] }));
app.use(express.urlencoded({ limit: '100mb', extended: true }));
// 🟢 เพิ่ม Endpoint พิเศษสำหรับให้ Cron Job มาปลุกเซิร์ฟเวอร์
app.get('/ping', (req, res) => {
    res.status(200).send('Server is awake! 🚀');
});

// =====================================================================
// 🔑 Google Credentials (ใช้ร่วมกันทั้ง BigQuery และ Google Sheets)
// =====================================================================
let gcpCredentials;
try {
    // พยายามอ่านค่าจาก Render Environment
    gcpCredentials = JSON.parse(process.env.GCP_CREDENTIALS);
} catch (err) {
    console.error("❌ ไม่พบตัวแปร GCP_CREDENTIALS หรือ JSON Format ผิดพลาด! เซิร์ฟเวอร์จะรันต่อ แต่ทุก Query จะ Error จนกว่าจะตั้งค่าให้ถูกต้อง");
    gcpCredentials = {};
}

const bigquery = new BigQuery({
    projectId: 'pro-analytics-db',
    credentials: gcpCredentials
});

// 🟢 Google Sheets client (ใช้แทน SpreadsheetApp ของ Apps Script เดิม)
// สำคัญ: ต้องแชร์สิทธิ์ "ดูได้ (Viewer)" ของสเปรดชีตทุกตัวด้านล่างให้กับอีเมลของ Service Account
// (ดูได้จาก gcpCredentials.client_email) ไม่เช่นนั้นจะได้ error 403 Permission denied ตอนอ่านชีต
let sheetsClientPromise = null;
function getSheetsClient() {
    if (!sheetsClientPromise) {
        const auth = new google.auth.GoogleAuth({
            credentials: gcpCredentials,
            scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly']
        });
        sheetsClientPromise = auth.getClient().then(authClient => google.sheets({ version: 'v4', auth: authClient }));
    }
    return sheetsClientPromise;
}

// อ่านทั้งชีตแบบ 2D array (เทียบเท่า getDataRange().getValues() / getDisplayValues())
async function getSheetValues(spreadsheetId, sheetName) {
    const sheets = await getSheetsClient();
    const resp = await sheets.spreadsheets.values.get({
        spreadsheetId,
        range: sheetName,
        valueRenderOption: 'FORMATTED_VALUE'
    });
    return resp.data.values || [];
}

// รหัสสเปรดชีตของ Master Data (ย้ายมาจาก Google Apps Script เดิม)
const SPREADSHEET_ID = '1-qmS7WmLjkbrDxcdo_Ep_FBRWMDpuQQoetKgJ9CQYMY';   // ใบหลัก: Sheet1 + Config_Zone
const ITEM_SHEET_ID = '1_Wg8BwUnw2Pnun1-JESeOGNGCz_ug5GIqlfefn5vikI';    // Master Item Status
const ZONE_SHEET_ID = '1lPb9Wd333Q6We6eB8jORJcHSmCfzIlSoseemzaqcZRw';    // Master Assign Location
const PICK_SHEET_ID = '1Nw8Y9XiCjbDHfBb8sQEkSTG0lrbcAyILZzGVNBANOfk';    // Master Pick Type
const UNIT_SHEET_ID = '16KsbwbbaqwPDAax-un7kqnabjmRtXhtMPHyu4wPyQJc';    // Master Unit/Pack Conversion

// =====================================================================
// 🟠 ส่วนที่ 1: สำหรับ "เว็บเก่า" (BigQuery) ต้องเป็น app.post เท่านั้น
// =====================================================================
app.post('/api/run', async (req, res) => {
    // 🛡️ ป้องกันเซิร์ฟเวอร์พังเวลาไม่มีข้อมูลส่งมา หรือคนอื่นเรียกผิดช่องทาง
    if (!req.body || !req.body.fn || !req.body.args) {
        return res.status(400).json({ success: false, message: "Invalid request: Missing fn or args" });
    }

    const { fn, args } = req.body;
    console.log(`\n[API Received] 👉 กำลังรันฟังก์ชัน: ${fn}`);

    try {
        let result;
        if (fn === 'apiGetMissedPickReport') result = await apiGetMissedPickReport(args[0], args[1]);
        else if (fn === 'saveActualToBigQuery') result = await saveActualToBigQuery(args[0]);
        else if (fn === 'saveDetailToBigQuery') result = await saveDetailToBigQuery(args[0]);
        else if (fn === 'getDailyDetailsFromDB') result = await getDailyDetailsFromDB(args[0], args[1]);
        else if (fn === 'apiGetDashboardSummary') result = await apiGetDashboardSummary(args[0], args[1]);
        else if (fn === 'apiGetMismatchReport') result = await apiGetMismatchReport(args[0], args[1]);
        else if (fn === 'apiGetWaveMonitoring') result = await apiGetWaveMonitoring(args[0], args[1]); // 🟢 เพิ่มบรรทัดนี้
        // 🟢🟢🟢 ฟังก์ชัน Master Data 6 ตัว ที่ย้ายมาจาก Google Apps Script (เดิมเรียก GAS_URL แล้ว 404) 🟢🟢🟢
        else if (fn === 'apiGetZoneRules') result = await apiGetZoneRules();
        else if (fn === 'apiGetDB') result = await apiGetDB();
        else if (fn === 'apiGetItem') result = await apiGetItem();
        else if (fn === 'apiGetZone') result = await apiGetZone();
        else if (fn === 'apiGetPick') result = await apiGetPick();
        else if (fn === 'apiGetUnit') result = await apiGetUnit();
        
        // --- ส่วนที่เพิ่มใหม่สำหรับจัดการ Capacity ---
        else if (fn === 'apiGetCapacity') {
            const dateFilter = buildDateFilter(args[0], args[1], `target_date`, 30, false);
            const sql = `SELECT target_date, owner, capacity FROM \`pro-analytics-db.logistics_db.daily_capacity\` WHERE 1=1 ${dateFilter}`;
            const [rows] = await bigquery.query({ query: sql });
            result = { success: true, data: rows };
        }
        else if (fn === 'apiSaveCapacity') {
            const { target_date, owner, capacity } = args[0];
            // ลบข้อมูลของวันและ BU นั้นออกก่อน เพื่ออัปเดตค่าใหม่ (Upsert)
            await bigquery.query({
                query: `DELETE FROM \`pro-analytics-db.logistics_db.daily_capacity\` WHERE target_date = '${target_date}' AND owner = '${owner}'`
            });
            await bigquery.query({
                query: `INSERT INTO \`pro-analytics-db.logistics_db.daily_capacity\` (target_date, owner, capacity) VALUES ('${target_date}', '${owner}', ${capacity})`
            });
            result = { success: true };
        }
        // ------------------------------------------

        else result = { success: false, message: `ยังไม่ได้เปิดใช้งานฟังก์ชัน ${fn}` };

        res.json(result);
    } catch (error) {
        console.error(`❌ [Error in ${fn}]`, error);
        res.status(500).json({ success: false, message: error.toString() });
    }
});

// =====================================================================
// 🟢 ส่วนที่ 2: สำหรับ "Management Dashboard" (Real-time SSE) ต้องเป็น app.get เท่านั้น
// =====================================================================
app.get('/api/run', (req, res) => {
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');

    const sendDashboardData = () => {
        // ข้อมูลจำลองสำหรับทดสอบ (อนาคตค่อยดึงจาก BigQuery มาเสียบตรงนี้)
        const sampleData = [
            {
                date: new Date().toISOString().split('T')[0],
                bu: "DP02",
                total_orders: 1500,
                completed_orders: 1420
            },
            {
                date: new Date().toISOString().split('T')[0],
                bu: "DM02",
                total_orders: 800,
                completed_orders: 750
            }
        ];
        res.write(`data: ${JSON.stringify(sampleData)}\n\n`);
    };

    // ส่งข้อมูลกลับไปทันที 1 ครั้ง และส่งซ้ำทุกๆ 5 วินาที
    sendDashboardData();
    const interval = setInterval(sendDashboardData, 5000);

    // ปิดท่อเมื่อหน้าต่าง Dashboard ถูกปิด
    req.on('close', () => {
        clearInterval(interval);
    });
});

// 🛡️ ช่วยตีความ startDate/endDate ที่หน้าเว็บอาจส่งมาเป็น 'All' / 'all' / ค่าว่าง ให้เป็น "ไม่กรองวันที่ (ย้อนหลัง N วัน)"
// dateExpr: นิพจน์ SQL ที่ใช้เทียบวันที่ (เช่น LEFT(CAST(PickDate AS STRING),10) หรือ DATE(Created_At))
// alreadyDate: true ถ้า dateExpr คืนค่าชนิด DATE อยู่แล้ว (เทียบ DATE_SUB ตรงๆได้), false ถ้าคืนค่าเป็น STRING
//              (กรณี STRING ต้องครอบด้วย PARSE_DATE ก่อนเทียบกับ DATE_SUB ไม่งั้น BigQuery จะ throw
//              "No matching signature for operator >= for argument types: STRING, DATE")
function buildDateFilter(startDate, endDate, dateExpr, days = 30, alreadyDate = false) {
    if (!startDate || startDate === 'All' || startDate === 'all' || !endDate || endDate === 'All' || endDate === 'all') {
        const cmpExpr = alreadyDate ? dateExpr : `PARSE_DATE('%Y-%m-%d', ${dateExpr})`;
        return ` AND ${cmpExpr} >= DATE_SUB(CURRENT_DATE(), INTERVAL ${days} DAY)`;
    }
    // BETWEEN เทียบกับ string literal ตรงๆ ใช้ได้ทั้งกรณี dateExpr เป็น STRING หรือ DATE
    // (BigQuery ทำ literal coercion ให้อัตโนมัติ)
    return ` AND ${dateExpr} BETWEEN '${startDate}' AND '${endDate}'`;
}

// =====================================================================
// 📦 1. ฟังก์ชันดึงรายงาน Missed Pick
// 🟢 แก้บัก (รอบนี้): โค้ดเดิมคำนวณ "ReqQty - ShippedQty" จากตาราง actual_fulfillment_v2 ตารางเดียว
// ซึ่งเป็นสูตรคนละแบบกับที่ Frontend ต้องการ (Frontend ใน renderShortagePivot()/fetchShortageFromDB()
// ต้องการฟิลด์ srcDate, own, itm, desc, z, pt, est, ship, missed_pick — เทียบ "แผนที่ต้องหยิบ (Estimated
// จาก fulfillment_details_v2)" กับ "ที่หยิบได้จริง (Shipped จาก actual_fulfillment_v2)" คนละตารางกัน)
// เดิมส่ง field ผิดชื่อไปหมด (date/pD/req/short) ทำให้หน้าเว็บอ่านค่า r.srcDate, r.est, r.missed_pick
// ไม่เจอ เลยโชว์ "-" และ 0 ทั้งคอลัมน์ ทั้งที่ query ไม่ error
// แก้โดย join Est (fulfillment_details_v2) กับ Act (actual_fulfillment_v2) เหมือนต้นฉบับ Apps Script เดิม
// =====================================================================
async function apiGetMissedPickReport(startDate, endDate) {
    const datasetId = 'logistics_db';

    // 🚨 ดักจับกรณีที่ Date Picker ส่งค่าคำว่า 'All'
    const dateFilter = buildDateFilter(startDate, endDate, `LEFT(CAST(PickDate AS STRING), 10)`);

    const sql = `
        WITH Est AS (
            SELECT
                LEFT(CAST(PickDate AS STRING), 10) as PickDate,
                UPPER(TRIM(CAST(Owner AS STRING))) as Owner,
                UPPER(TRIM(CAST(Item AS STRING))) as Item,
                MAX(Description) as Description, MAX(Zone) as Zone, MAX(PickType) as PickType,
                SUM(SAFE_CAST(AllocatedQty AS FLOAT64)) as est_qty
            FROM \`pro-analytics-db.${datasetId}.fulfillment_details_v2\`
            WHERE 1=1 ${dateFilter}
            GROUP BY 1, 2, 3
        ),
        Act AS (
            SELECT
                LEFT(CAST(PickDate AS STRING), 10) as PickDate,
                UPPER(TRIM(CAST(Owner AS STRING))) as Owner,
                UPPER(TRIM(CAST(Item AS STRING))) as Item,
                SUM(SAFE_CAST(ShippedQty AS FLOAT64)) as ship_qty
            FROM \`pro-analytics-db.${datasetId}.actual_fulfillment_v2\`
            WHERE 1=1 ${dateFilter}
            GROUP BY 1, 2, 3
        ),
        ValidDates AS (
            SELECT PickDate FROM Act GROUP BY PickDate HAVING SUM(ship_qty) > 0
        )
        SELECT
            e.PickDate as srcDate, e.Owner as own, e.Item as itm, e.Description as itm_desc,
            e.Zone as z, e.PickType as pt,
            e.est_qty as est, IFNULL(a.ship_qty, 0) as ship,
            (e.est_qty - IFNULL(a.ship_qty, 0)) as missed_pick
        FROM Est e
        INNER JOIN ValidDates vd ON e.PickDate = vd.PickDate
        LEFT JOIN Act a ON e.PickDate = a.PickDate AND e.Owner = a.Owner AND e.Item = a.Item
        WHERE (e.est_qty - IFNULL(a.ship_qty, 0)) > 0
        ORDER BY srcDate DESC, missed_pick DESC
    `;

    console.log(`\n[Query] Missed Pick เงื่อนไขเวลา: startDate=${startDate}`);
    try {
        const [rows] = await bigquery.query({ query: sql });

        // 🛠️ ส่งฟิลด์ให้ตรงกับที่ Frontend (renderShortagePivot) ใช้จริง: srcDate, own, itm, desc, z, pt, est, ship, missed_pick
        // และเผื่อชื่อย่อ/ชื่อเต็มแบบอื่นไว้ด้วยเผื่อโค้ดส่วนอื่นอ้างถึง
        const formattedData = rows.map(r => {
            const est = r.est || 0;
            const ship = r.ship || 0;
            const missed = r.missed_pick || 0;

            return {
                srcDate: r.srcDate, date: r.srcDate, pD: r.srcDate,
                own: r.own, owner: r.own,
                itm: r.itm, item: r.itm,
                desc: r.itm_desc, description: r.itm_desc,
                z: r.z, zone: r.z,
                pt: r.pt, pickType: r.pt, pick: r.pt,
                est, estimated: est, req: est, reqQty: est,
                ship, shippedQty: ship,
                missed_pick: missed, missedPick: missed, short: missed, shortage: missed
            };
        });

        console.log(`✅ ประมวลผลเสร็จสิ้น เจอ Missed Pick ทั้งหมด ${formattedData.length} รายการ`);
        return { success: true, data: formattedData };
    } catch (err) {
        console.error("❌ SQL Error (Missed Pick):", err);
        throw err;
    }
}

// =====================================================================
// 📦 2. ฟังก์ชันอัปโหลดหน้า Actual
// =====================================================================
async function saveActualToBigQuery(reportData) {
    if (!reportData || reportData.length === 0) return { success: true };
    const datasetId = 'logistics_db';
    const tableId = 'actual_fulfillment_v2';
    const targetDate = reportData[0].pD || reportData[0].pickDate || reportData[0].PickDate;

    if (targetDate) {
        console.log(`[Delete] กำลังล้างข้อมูลเก่าของวันที่ ${targetDate}...`);
        await bigquery.query({ query: `DELETE FROM \`pro-analytics-db.${datasetId}.${tableId}\` WHERE LEFT(CAST(PickDate AS STRING), 10) = '${targetDate}'` });
    }

    const dataForBQ = reportData.map(row => ({
        OrderDate: row.oD || row.OrderDate || "", PickDate: row.pD || row.pickDate || row.PickDate || "", DeliveryDate: row.dD || row.DeliveryDate || "",
        OrderNo: String(row.ord || row.OrderNo || ""), LineNo: String(row.line || row.LineNo || ""), Owner: row.own || row.Owner || "",
        Item: row.itm || row.Item || "", Description: row.desc || row.Description || "", Zone: row.z || row.Zone || "",
        LocType: row.loc || row.LocType || "", PickType: row.pt || row.PickType || "", PickPackQty: parseFloat(row.pkQ || row.PickPackQty)||1,
        PickUnits: parseFloat(row.pu || row.PickUnits)||0, ReqQty: parseFloat(row.req || row.ReqQty)||0, ReqPallet: parseFloat(row.reqPlt || row.ReqPallet)||0,
        ShippedQty: parseFloat(row.ship || row.ShippedQty)||0, ShippedPallet: parseFloat(row.shipPlt || row.ShippedPallet)||0,
        OrderUom: row.oUom || row.OrderUom || "-", BaseUom: row.baseUom || row.BaseUom || "-", Shortage: parseFloat(row.short || row.Shortage)||0,
        Remark: row.remark || row.Remark || "-", Status: row.res || row.Status || "-", Timestamp: new Date().toISOString()
    }));

    const ndjson = dataForBQ.map(obj => JSON.stringify(obj)).join('\n');
    const tempFile = `./temp_act_${Date.now()}.json`;

    try {
        fs.writeFileSync(tempFile, ndjson);
        console.log(`[Upload] กำลังอัปโหลดไฟล์ Actual เข้า BigQuery...`);
        await bigquery.dataset(datasetId).table(tableId).load(tempFile, {
            sourceFormat: 'NEWLINE_DELIMITED_JSON',
            writeDisposition: 'WRITE_APPEND',
            schemaUpdateOptions: ['ALLOW_FIELD_ADDITION']
        });
        console.log(`✅ [Success] อัปโหลด Actual เสร็จสิ้น!`);
    } finally {
        if (fs.existsSync(tempFile)) fs.unlinkSync(tempFile);
    }
    return { success: true };
}

// =====================================================================
// 📦 3. ฟังก์ชันดึงแผนตั้งต้น
// =====================================================================
async function getDailyDetailsFromDB(targetDate, stage) {
    const datasetId = 'logistics_db';
    let tableId = stage === 'actual' ? 'actual_fulfillment_v2' : 'fulfillment_details_v2';
    console.log(`[Query] ดึงข้อมูลวันที่ ${targetDate} จาก ${tableId}...`);

    const sql = `
        SELECT * FROM \`pro-analytics-db.${datasetId}.${tableId}\`
        WHERE LEFT(CAST(PickDate AS STRING), 10) = '${targetDate}'
    `;

    const [rows] = await bigquery.query({ query: sql });

    const cleanRows = rows.map(r => {
        if (r.PickDate && typeof r.PickDate === 'object') r.PickDate = r.PickDate.value;
        if (r.OrderDate && typeof r.OrderDate === 'object') r.OrderDate = r.OrderDate.value;
        if (r.DeliveryDate && typeof r.DeliveryDate === 'object') r.DeliveryDate = r.DeliveryDate.value;
        return r;
    });

    console.log(`✅ ดึงข้อมูลสำเร็จ ${cleanRows.length} บรรทัด`);
    return { success: true, data: cleanRows };
}

// =====================================================================
// 📦 4. ฟังก์ชันอัปโหลดหน้า Estimated
// =====================================================================
async function saveDetailToBigQuery(reportData) {
    if (!reportData || reportData.length === 0) return { success: true };
    const datasetId = 'logistics_db';
    const tableId = 'fulfillment_details_v2';
    const targetDate = reportData[0].pD || reportData[0].pickDate || reportData[0].PickDate;

    if (targetDate) {
        console.log(`[Delete] กำลังล้างข้อมูลเก่าของวันที่ ${targetDate}...`);
        await bigquery.query({ query: `DELETE FROM \`pro-analytics-db.${datasetId}.${tableId}\` WHERE LEFT(CAST(PickDate AS STRING), 10) = '${targetDate}'` });
    }

    const dataForBQ = reportData.map(row => ({
        OrderDate: row.oD || row.OrderDate || "", PickDate: row.pD || row.pickDate || row.PickDate || "", DeliveryDate: row.dD || row.DeliveryDate || "",
        OrderNo: String(row.ord || row.OrderNo || ""), LineNo: String(row.line || row.LineNo || ""),
        Owner: row.own || row.Owner || "", Item: row.itm || row.Item || "", Description: row.desc || row.Description || "",
        Zone: row.z || row.Zone || "", LocType: row.loc || row.LocType || "", PickType: row.pt || row.PickType || "",
        PickPackQty: parseInt(row.pkQ) || 1, PickUnits: parseFloat(row.pu) || 0, OrderQty: parseFloat(row.req || row.OrderQty) || 0,
        OrderUom: row.oUom || "-", BaseUom: row.baseUom || "-", ReqPallet: parseFloat(row.reqPlt || row.ReqPallet) || 0,
        AvailBefore: parseFloat(row.av || row.AvailBefore) || 0, AllocatedQty: parseFloat(row.alloc || row.AllocatedQty) || 0,
        AllocPallet: parseFloat(row.allocPlt || row.AllocPallet) || 0, Remark: row.remark || row.Remark || "-",
        ItemStatus: row.stat || row.ItemStatus || "-", Result: row.res || row.Result || "-", Timestamp: new Date().toISOString()
    }));

    const ndjson = dataForBQ.map(obj => JSON.stringify(obj)).join('\n');
    const tempFile = `./temp_est_${Date.now()}.json`;

    try {
        fs.writeFileSync(tempFile, ndjson);
        console.log(`[Upload] กำลังอัปโหลดไฟล์ Estimated เข้า BigQuery...`);
        await bigquery.dataset(datasetId).table(tableId).load(tempFile, {
            sourceFormat: 'NEWLINE_DELIMITED_JSON',
            writeDisposition: 'WRITE_APPEND',
            schemaUpdateOptions: ['ALLOW_FIELD_ADDITION']
        });
        console.log(`✅ [Success] อัปโหลด Estimated เสร็จสิ้น!`);
    } finally {
        if (fs.existsSync(tempFile)) fs.unlinkSync(tempFile);
    }
    return { success: true };
}

// =====================================================================
// 📦 5. ฟังก์ชันดึงสรุป Dashboard จาก BigQuery โดยตรง
// =====================================================================
async function apiGetDashboardSummary(startDate, endDate) {
    const datasetId = 'logistics_db';
    // 🟢 แก้บั๊ก: เดิมไม่ดัก 'All' และ (รอบก่อนหน้า) ลืมครอบ PARSE_DATE ทำให้ BigQuery throw
    // "No matching signature for operator >= for argument types: STRING, DATE" ตอนไม่ระบุวันที่
    const dateFilter = buildDateFilter(startDate, endDate, `LEFT(CAST(PickDate AS STRING), 10)`);

    const sql = `
    WITH Est AS (
        SELECT
            LEFT(CAST(PickDate AS STRING), 10) as PickDate, UPPER(TRIM(CAST(Owner AS STRING))) as Owner,
            SUM(CAST(OrderQty AS FLOAT64)) as req, SUM(CAST(AllocatedQty AS FLOAT64)) as alloc,
            SUM(CAST(PickUnits AS FLOAT64)) as pu, SUM(CAST(ReqPallet AS FLOAT64)) as plt
        FROM \`pro-analytics-db.${datasetId}.fulfillment_details_v2\` WHERE 1=1 ${dateFilter} GROUP BY 1, 2
    ),
    Act AS (
        SELECT
            LEFT(CAST(PickDate AS STRING), 10) as PickDate, UPPER(TRIM(CAST(Owner AS STRING))) as Owner,
            SUM(CAST(ShippedQty AS FLOAT64)) as ship, SUM(CAST(ShippedPallet AS FLOAT64)) as ship_plt,
            SUM(CAST(Shortage AS FLOAT64)) as actShort, COUNT(DISTINCT OrderNo) as ordTotal,
            (COUNT(DISTINCT OrderNo) - COUNT(DISTINCT CASE WHEN Status NOT IN ('Shipped Full', 'Over Shipped') THEN OrderNo END)) as ordFull
        FROM \`pro-analytics-db.${datasetId}.actual_fulfillment_v2\` WHERE 1=1 ${dateFilter} GROUP BY 1, 2
    ),
    Merged AS (
        SELECT
            COALESCE(e.PickDate, a.PickDate) as PickDate, COALESCE(e.Owner, a.Owner) as Owner,
            IFNULL(e.req, 0) as req, IFNULL(e.alloc, 0) as alloc, IFNULL(e.pu, 0) as pu, IFNULL(e.plt, 0) as plt,
            IFNULL(a.ship, 0) as ship, IFNULL(a.ship_plt, 0) as ship_plt, IFNULL(a.actShort, 0) as actShort,
            IFNULL(a.ordTotal, 0) as ordTotal, IFNULL(a.ordFull, 0) as ordFull
        FROM Est e FULL OUTER JOIN Act a ON e.PickDate = a.PickDate AND e.Owner = a.Owner
    )
    SELECT
        PickDate as date, 'actual' as type, SUM(req) as req, SUM(alloc) as alloc, SUM(ship) as ship,
        SUM(ordTotal) as orders, SUM(plt) as pallets, SUM(pu) as pcsPick,
        TO_JSON_STRING(ARRAY_AGG(STRUCT(Owner as own, req, alloc, ship, actShort, ordTotal, ordFull, pu, ship_plt as plt))) as ownerJson_raw
    FROM Merged GROUP BY 1, 2 ORDER BY date DESC
    `;

    console.log("[Query] กำลังสั่ง BigQuery คำนวณยอด Dashboard...");
    try {
        const [rows] = await bigquery.query({ query: sql });
        const formattedRows = rows.map(r => {
            let ownerMap = {}; let rawArr = [];
            try { rawArr = JSON.parse(r.ownerJson_raw || '[]'); } catch(e){}

            let tOrd = 0, tFull = 0;
            rawArr.forEach(item => {
                ownerMap[item.own || 'UNKNOWN'] = item;
                tOrd += (item.ordTotal || 0); tFull += (item.ordFull || 0);
            });

            return {
                date: r.date, type: r.type, req: r.req, alloc: r.alloc, ship: r.ship, orders: r.orders,
                pallets: r.pallets, pcsPick: r.pcsPick, orderSla: tOrd > 0 ? (tFull / tOrd) * 100 : 0,
                ownerJson: JSON.stringify(ownerMap)
            };
        });
        console.log(`✅ คำนวณสรุป Dashboard เสร็จสิ้น ${formattedRows.length} วัน`);
        return { success: true, data: formattedRows };
    } catch (error) { throw error; }
}

// =====================================================================
// 📦 6. ฟังก์ชันดึงรายงานเบิกไม่ตรงแพ็คไซต์
// =====================================================================
async function apiGetMismatchReport(startDate, endDate) {
    const datasetId = 'logistics_db';
    // 🟢 แก้บั๊ก: ดัก 'All' และครอบ PARSE_DATE เหมือน apiGetDashboardSummary
    const dateFilter = buildDateFilter(startDate, endDate, `LEFT(CAST(PickDate AS STRING), 10)`);

    const sql = `
      SELECT
        LEFT(CAST(PickDate AS STRING), 10) as srcDate,
        TRIM(CAST(Owner AS STRING)) as own,
        TRIM(CAST(OrderNo AS STRING)) as ord,
        TRIM(CAST(Item AS STRING)) as itm,
        MAX(Description) as \`desc\`,
        MAX(Zone) as z,
        MAX(PickType) as pt,
        SUM(CAST(OrderQty AS FLOAT64)) as req_qty,
        SUM(CAST(PickUnits AS FLOAT64)) as pu
      FROM \`pro-analytics-db.${datasetId}.fulfillment_details_v2\`
      WHERE Remark = 'ไม่ตรง pack size' ${dateFilter}
      GROUP BY 1, 2, 3, 4
      ORDER BY srcDate DESC, req_qty DESC
    `;

    console.log("[Query] กำลังดึงข้อมูล Pack Size Mismatch...");
    try {
        const [rows] = await bigquery.query({ query: sql });
        return { success: true, data: rows };
    } catch (err) {
        console.error("❌ SQL Error (Mismatch):", err);
        throw err;
    }
}

// =====================================================================
// 📦 7. ฟังก์ชันดึงข้อมูล Wave Monitoring (ดึงข้อมูล Pick, QC, Ship, On-time)
// =====================================================================
async function apiGetWaveMonitoring(startDate, endDate) {
    const datasetId = 'logistics_db';
    // 🟢 แก้บั๊กหลัก: เดิมเทียบ DATE(Created_At) กับ 'All' ตรงๆ ทำให้ BigQuery throw
    // "Invalid date: 'All'" แล้วเด้ง 500 ทุกครั้งที่กด "วันที่ All"
    // alreadyDate=true เพราะ DATE(Created_At) คืนค่าชนิด DATE อยู่แล้ว ไม่ต้องครอบ PARSE_DATE ซ้ำ
    const dateFilter = buildDateFilter(startDate, endDate, `DATE(Created_At)`, 7, true);

    // 🟢 เพิ่ม Status_Check และ Time_Check พร้อมคำนวณ SLA -30 นาที
    const sql = `
        WITH LatestOrders AS (
            SELECT * FROM \`pro-analytics-db.${datasetId}.wave_monitoring\`
            WHERE 1=1 ${dateFilter}
            QUALIFY ROW_NUMBER() OVER(PARTITION BY Order_Number ORDER BY Created_At DESC) = 1
        ),
        ParsedTimes AS (
            SELECT
                Created_At,
                Order_Number,
                Status_Pick,
                Status_Check, -- ดึงสถานะ QC
                Time_Check,   -- ดึงเวลา QC
                Status_Load,
                SAFE_CAST(CONCAT(CAST(Planned_Pick_Date AS STRING), ' ', REPLACE(TRIM(Planned_Load_Time), '.', ':'), ':00') AS DATETIME) AS target_load_time,
                DATETIME_SUB(SAFE_CAST(CONCAT(CAST(Planned_Pick_Date AS STRING), ' ', REPLACE(TRIM(Planned_Load_Time), '.', ':'), ':00') AS DATETIME), INTERVAL 90 MINUTE) AS target_pick_time,
                -- 🚨 คำนวณ SLA ของ QC (ก่อนเวลาโหลด 30 นาที)
                DATETIME_SUB(SAFE_CAST(CONCAT(CAST(Planned_Pick_Date AS STRING), ' ', REPLACE(TRIM(Planned_Load_Time), '.', ':'), ':00') AS DATETIME), INTERVAL 30 MINUTE) AS target_qc_time
            FROM LatestOrders
        )
        SELECT
            DATE(Created_At) AS work_date,
            COUNT(DISTINCT Order_Number) AS total_orders,
            COUNT(DISTINCT CASE WHEN LOWER(TRIM(Status_Pick)) = 'done' THEN Order_Number END) AS picked_orders,
            -- 🚨 นับจำนวนที่ผ่าน QC
            COUNT(DISTINCT CASE WHEN LOWER(TRIM(Status_Check)) = 'done' THEN Order_Number END) AS qc_orders,
            COUNT(DISTINCT CASE WHEN LOWER(TRIM(Status_Load)) = 'done' THEN Order_Number END) AS shipped_orders,

            COUNT(DISTINCT CASE WHEN LOWER(TRIM(Status_Pick)) != 'done' AND target_pick_time IS NOT NULL AND CURRENT_DATETIME('Asia/Bangkok') > target_pick_time THEN Order_Number END) AS late_pick_orders,
            -- 🚨 นับจำนวนบิลที่เลยเวลา QC ไปแล้ว (Late QC)
            COUNT(DISTINCT CASE WHEN LOWER(TRIM(Status_Check)) != 'done' AND target_qc_time IS NOT NULL AND CURRENT_DATETIME('Asia/Bangkok') > target_qc_time THEN Order_Number END) AS late_qc_orders,
            COUNT(DISTINCT CASE WHEN LOWER(TRIM(Status_Load)) != 'done' AND target_load_time IS NOT NULL AND CURRENT_DATETIME('Asia/Bangkok') > target_load_time THEN Order_Number END) AS late_load_orders,

            MAX(CASE WHEN LOWER(TRIM(Status_Pick)) != 'done' AND target_pick_time IS NOT NULL AND CURRENT_DATETIME('Asia/Bangkok') > target_pick_time THEN TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), TIMESTAMP(target_pick_time, 'Asia/Bangkok'), MINUTE) ELSE 0 END) AS max_pick_delay_mins,
            MAX(CASE WHEN LOWER(TRIM(Status_Load)) != 'done' AND target_load_time IS NOT NULL AND CURRENT_DATETIME('Asia/Bangkok') > target_load_time THEN TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), TIMESTAMP(target_load_time, 'Asia/Bangkok'), MINUTE) ELSE 0 END) AS max_load_delay_mins
        FROM ParsedTimes
        GROUP BY DATE(Created_At)
        ORDER BY work_date DESC
    `;

    console.log("[Query] กำลังดึงข้อมูลจาก wave_monitoring (แยก Pick/QC/Load)...");
    try {
        const [rows] = await bigquery.query({ query: sql });
        return { success: true, data: rows };
    } catch (err) {
        console.error("❌ SQL Error (Wave Monitoring):", err);
        throw err;
    }
}

// =====================================================================
// 🟢🟢🟢 8-13. ฟังก์ชัน Master Data ที่ย้ายมาจาก Google Apps Script เดิม (แก้ปัญหา 404) 🟢🟢🟢
// เดิมฟังก์ชันเหล่านี้อยู่บน Google Apps Script (GAS_URL) และอ่านข้อมูลผ่าน SpreadsheetApp
// ย้ายมาอ่านผ่าน Google Sheets API v4 โดยใช้ Service Account เดียวกับที่ใช้คุย BigQuery
// ⚠️ ต้องแชร์สิทธิ์ "ผู้ดู (Viewer)" ของสเปรดชีตทุกใบด้านล่างให้กับอีเมลใน gcpCredentials.client_email
// =====================================================================

// 8. ข้อมูลร้านค้า/สาขา (Sheet1 ของสเปรดชีตหลัก)
async function apiGetDB() {
    try {
        const values = await getSheetValues(SPREADSHEET_ID, 'Sheet1');
        return { s: true, d: values };
    } catch (e) {
        console.error("❌ [apiGetDB] Error:", e);
        return { s: false, m: e.toString() };
    }
}

// 9. Master Item Status: owner_code -> status (คอลัมน์ A=owner, B=code, E=status), เริ่มแถวที่ 2
async function apiGetItem() {
    try {
        const values = await getSheetValues(ITEM_SHEET_ID, 'Sheet1');
        const res = {};
        for (let i = 1; i < values.length; i++) {
            const row = values[i] || [];
            const owner = (row[0] || '').toString().trim();
            const code = (row[1] || '').toString().trim();
            if (owner && code) res[`${owner}_${code}`] = (row[4] || '').toString().trim();
        }
        return { s: true, d: res };
    } catch (e) {
        console.error("❌ [apiGetItem] Error:", e);
        return { s: false, m: e.toString() };
    }
}

// 10. Master Assign Location: owner_item -> zone (2 ตัวอักษรแรกของ LocType คอลัมน์ D), เริ่มแถวที่ 2
async function apiGetZone() {
    try {
        const values = await getSheetValues(ZONE_SHEET_ID, 'Sheet1');
        const res = {};
        for (let i = 1; i < values.length; i++) {
            const row = values[i] || [];
            const owner = (row[0] || '').toString().trim();
            const item = (row[1] || '').toString().trim();
            const loc = (row[3] || '').toString().trim();
            if (owner && item) {
                res[`${owner}_${item}`] = loc.length >= 2 ? loc.substring(0, 2).toUpperCase() : (loc ? loc.toUpperCase() : 'ไม่ได้กำหนด');
            }
        }
        return { s: true, d: res };
    } catch (e) {
        console.error("❌ [apiGetZone] Error:", e);
        return { s: false, m: e.toString() };
    }
}

// 11. Master Pick Type: owner_item -> pickType (คอลัมน์ B=owner, C=item, ลำดับที่ 272=pickType), sheet 'Data'
async function apiGetPick() {
    try {
        const values = await getSheetValues(PICK_SHEET_ID, 'Data');
        const res = {};
        for (let i = 1; i < values.length; i++) {
            const row = values[i] || [];
            const owner = (row[1] || '').toString().trim();   // คอลัมน์ B (index 1)
            const item = (row[2] || '').toString().trim();    // คอลัมน์ C (index 2)
            const pickType = (row[271] || '').toString().trim(); // คอลัมน์ที่ 272 (index 271)
            if (owner && item) res[`${owner}_${item}`] = pickType;
        }
        return { s: true, d: res };
    } catch (e) {
        console.error("❌ [apiGetPick] Error:", e);
        return { s: false, m: e.toString() };
    }
}

// 12. Master Unit/Pack Conversion: packKey -> {uom1, qty1, uom2, qty2, uom3, qty3, palletQty}, sheet 'Data'
// หา header row ที่มีคำว่า PACKKEY อยู่ในแถวแรกๆ (10 แถว) แล้ว map ชื่อคอลัมน์ -> index
async function apiGetUnit() {
    try {
        const values = await getSheetValues(UNIT_SHEET_ID, 'Data');
        const res = {};
        let headerRowIdx = -1;
        const colMap = {};

        for (let r = 0; r < Math.min(10, values.length); r++) {
            const row = values[r] || [];
            for (let c = 0; c < row.length; c++) {
                if ((row[c] || '').toString().trim().toUpperCase() === 'PACKKEY') { headerRowIdx = r; break; }
            }
            if (headerRowIdx !== -1) break;
        }

        if (headerRowIdx !== -1) {
            const headerRow = values[headerRowIdx] || [];
            for (let c = 0; c < headerRow.length; c++) {
                if (headerRow[c]) colMap[headerRow[c].toString().trim().toUpperCase()] = c;
            }

            for (let i = headerRowIdx + 1; i < values.length; i++) {
                const row = values[i] || [];
                const packKey = (row[colMap['PACKKEY']] || '').toString().trim();
                if (packKey) {
                    res[packKey] = {
                        uom1: (row[colMap['PACKUOM1']] || '').toString().trim().toUpperCase(),
                        qty1: parseFloat(row[colMap['CASECNT']]) || 1,
                        uom2: (row[colMap['PACKUOM2']] || '').toString().trim().toUpperCase(),
                        qty2: parseFloat(row[colMap['INNERPACK']]) || 1,
                        uom3: (row[colMap['PACKUOM3']] || '').toString().trim().toUpperCase(),
                        qty3: parseFloat(row[colMap['QTY']]) || 1,
                        palletQty: parseFloat(row[colMap['PALLET']]) || 0
                    };
                }
            }
        }
        return { s: true, d: res };
    } catch (e) {
        console.error("❌ [apiGetUnit] Error:", e);
        return { s: false, m: e.toString() };
    }
}

// 13. Zone Rules (Config_Zone ของสเปรดชีตหลัก): [{prefix, zone, loc}, ...]
async function apiGetZoneRules() {
    try {
        const values = await getSheetValues(SPREADSHEET_ID, 'Config_Zone');
        const rules = [];
        for (let i = 1; i < values.length; i++) {
            const row = values[i] || [];
            if (row[0]) rules.push({ prefix: (row[0] || '').toString(), zone: (row[1] || '').toString(), loc: (row[2] || '').toString() });
        }
        return { s: true, d: rules };
    } catch (e) {
        console.error("❌ [apiGetZoneRules] Error:", e);
        return { s: false, m: e.toString() };
    }
}
// 🟢👆 จบส่วนที่ต้องวาง 👆🟢

app.listen(port, () => {
    console.log(`\n=================================================`);
    console.log(`🚀 Backend Server เปิดพร้อมทำงานแล้ว!`);
    console.log(`👉 รอรับคำสั่งอยู่ที่: http://localhost:${port}/api/run`);
    console.log(`=================================================\n`);
});
