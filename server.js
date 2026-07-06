const express = require('express');
const cors = require('cors');
const { BigQuery } = require('@google-cloud/bigquery');
const fs = require('fs');

const app = express();
// ใช้ process.env.PORT เพื่อให้รองรับระบบอัตโนมัติของ Render ได้ดีขึ้น
const port = process.env.PORT || 3000;

app.use(cors()); 
app.use(express.json({ limit: '100mb', type: ['application/json', 'text/plain'] })); 
app.use(express.urlencoded({ limit: '100mb', extended: true }));

let gcpCredentials;
try {
    // พยายามอ่านค่าจาก Render Environment
    gcpCredentials = JSON.parse(process.env.GCP_CREDENTIALS);
} catch (err) {
    console.error("❌ ไม่พบตัวแปร GCP_CREDENTIALS หรือ JSON Format ผิดพลาด!");
    // ใส่ Fallback ไว้ป้องกันแอปแครช (แต่อาจจะคิวรีข้อมูลไม่ได้จนกว่าจะใส่ Key)
    gcpCredentials = {}; 
}

const bigquery = new BigQuery({
    projectId: 'pro-analytics-db', 
    credentials: gcpCredentials
});

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

// =====================================================================
// 📦 1. ฟังก์ชันดึงรายงาน Missed Pick
// =====================================================================
async function apiGetMissedPickReport(startDate, endDate) {
    const datasetId = 'logistics_db';
    let dateFilter = "";
    
    if (startDate && endDate) dateFilter = ` AND LEFT(CAST(PickDate AS STRING), 10) BETWEEN '${startDate}' AND '${endDate}'`;
    else if (startDate) dateFilter = ` AND LEFT(CAST(PickDate AS STRING), 10) >= '${startDate}'`;
    else if (endDate) dateFilter = ` AND LEFT(CAST(PickDate AS STRING), 10) <= '${endDate}'`;
    else dateFilter = ` AND PARSE_DATE('%Y-%m-%d', LEFT(CAST(PickDate AS STRING), 10)) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)`;

    // SQL สำหรับดึงและคำนวณ % ต่างๆ (นับเป็นบิล)
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
                Status_Load,
                -- แปลงรูปแบบเวลาที่มีจุด (.) ให้เป็น Colon (:) ป้องกัน BigQuery Error 500
                COALESCE(
                    SAFE_CAST(TRIM(Planned_Load_Time) AS DATETIME), 
                    DATETIME(Planned_Pick_Date, SAFE_CAST(REPLACE(TRIM(Planned_Load_Time), '.', ':') AS TIME))
                ) AS target_time
            FROM LatestOrders
        )
        SELECT 
            DATE(Created_At) AS work_date,
            COUNT(DISTINCT Order_Number) AS total_orders,
            COUNT(DISTINCT CASE WHEN LOWER(TRIM(Status_Pick)) = 'done' THEN Order_Number END) AS picked_orders,
            COUNT(DISTINCT CASE WHEN LOWER(TRIM(Status_Load)) = 'done' THEN Order_Number END) AS shipped_orders,
            COUNT(DISTINCT CASE 
                WHEN LOWER(TRIM(Status_Load)) != 'done' 
                     AND CURRENT_DATETIME('Asia/Bangkok') > target_time
                THEN Order_Number 
            END) AS late_orders,
            MAX(CASE 
                WHEN LOWER(TRIM(Status_Load)) != 'done' 
                     AND CURRENT_DATETIME('Asia/Bangkok') > target_time
                THEN TIMESTAMP_DIFF(CURRENT_TIMESTAMP('Asia/Bangkok'), TIMESTAMP(target_time, 'Asia/Bangkok'), MINUTE)
            END) AS max_delay_mins
        FROM ParsedTimes
        GROUP BY DATE(Created_At)
        ORDER BY work_date DESC
    `;
    
    const [rows] = await bigquery.query({ query: sql });
    return { success: true, data: rows };
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
    let dateFilter = "";
    if (startDate && endDate) dateFilter = ` AND LEFT(CAST(PickDate AS STRING), 10) BETWEEN '${startDate}' AND '${endDate}'`;
    else dateFilter = ` AND PARSE_DATE('%Y-%m-%d', LEFT(CAST(PickDate AS STRING), 10)) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)`;

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
    let dateFilter = "";
    if (startDate && endDate) dateFilter = ` AND LEFT(CAST(PickDate AS STRING), 10) BETWEEN '${startDate}' AND '${endDate}'`;
    else dateFilter = ` AND PARSE_DATE('%Y-%m-%d', LEFT(CAST(PickDate AS STRING), 10)) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)`;

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
// 📦 7. ฟังก์ชันดึงข้อมูล Wave Monitoring (ดึงข้อมูล Pick, Ship, On-time)
// =====================================================================
async function apiGetWaveMonitoring(startDate, endDate) {
    const datasetId = 'logistics_db';
    let dateFilter = "";
    
    if (startDate && endDate) {
        dateFilter = ` AND DATE(Created_At) BETWEEN '${startDate}' AND '${endDate}'`;
    } else {
        dateFilter = ` AND DATE(Created_At) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)`;
    }

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
                Status_Load,
                SAFE_CAST(CONCAT(CAST(Planned_Pick_Date AS STRING), ' ', REPLACE(TRIM(Planned_Load_Time), '.', ':'), ':00') AS DATETIME) AS target_load_time,
                -- กำหนดเวลาเป้าหมาย Pick (เช่น เสร็จก่อน Load 2 ชม.)
                DATETIME_SUB(SAFE_CAST(CONCAT(CAST(Planned_Pick_Date AS STRING), ' ', REPLACE(TRIM(Planned_Load_Time), '.', ':'), ':00') AS DATETIME), INTERVAL 2 HOUR) AS target_pick_time
            FROM LatestOrders
        )
        SELECT 
            DATE(Created_At) AS work_date,
            COUNT(DISTINCT Order_Number) AS total_orders,
            COUNT(DISTINCT CASE WHEN LOWER(TRIM(Status_Pick)) = 'done' THEN Order_Number END) AS picked_orders,
            COUNT(DISTINCT CASE WHEN LOWER(TRIM(Status_Load)) = 'done' THEN Order_Number END) AS shipped_orders,

            -- นับออเดอร์ที่ช้า
            COUNT(DISTINCT CASE WHEN LOWER(TRIM(Status_Pick)) != 'done' AND target_pick_time IS NOT NULL AND CURRENT_DATETIME('Asia/Bangkok') > target_pick_time THEN Order_Number END) AS late_pick_orders,
            COUNT(DISTINCT CASE WHEN LOWER(TRIM(Status_Load)) != 'done' AND target_load_time IS NOT NULL AND CURRENT_DATETIME('Asia/Bangkok') > target_load_time THEN Order_Number END) AS late_load_orders,

            -- คำนวณเวลาที่ช้าที่สุด (Delay)
            MAX(CASE WHEN LOWER(TRIM(Status_Pick)) != 'done' AND target_pick_time IS NOT NULL AND CURRENT_DATETIME('Asia/Bangkok') > target_pick_time THEN TIMESTAMP_DIFF(CURRENT_TIMESTAMP('Asia/Bangkok'), TIMESTAMP(target_pick_time, 'Asia/Bangkok'), MINUTE) ELSE 0 END) AS max_pick_delay_mins,
            MAX(CASE WHEN LOWER(TRIM(Status_Load)) != 'done' AND target_load_time IS NOT NULL AND CURRENT_DATETIME('Asia/Bangkok') > target_load_time THEN TIMESTAMP_DIFF(CURRENT_TIMESTAMP('Asia/Bangkok'), TIMESTAMP(target_load_time, 'Asia/Bangkok'), MINUTE) ELSE 0 END) AS max_load_delay_mins,

            -- คำนวณเวลาที่ทำล่วงหน้า (Early)
            MIN(CASE WHEN LOWER(TRIM(Status_Pick)) != 'done' AND target_pick_time IS NOT NULL AND CURRENT_DATETIME('Asia/Bangkok') <= target_pick_time THEN TIMESTAMP_DIFF(TIMESTAMP(target_pick_time, 'Asia/Bangkok'), CURRENT_TIMESTAMP('Asia/Bangkok'), MINUTE) ELSE NULL END) AS min_pick_early_mins,
            MIN(CASE WHEN LOWER(TRIM(Status_Load)) != 'done' AND target_load_time IS NOT NULL AND CURRENT_DATETIME('Asia/Bangkok') <= target_load_time THEN TIMESTAMP_DIFF(TIMESTAMP(target_load_time, 'Asia/Bangkok'), CURRENT_TIMESTAMP('Asia/Bangkok'), MINUTE) ELSE NULL END) AS min_load_early_mins

        FROM ParsedTimes
        GROUP BY DATE(Created_At)
        ORDER BY work_date DESC
    `;
    
    console.log("[Query] กำลังดึงข้อมูลจาก wave_monitoring (แยก Pick/Load)...");
    try {
        const [rows] = await bigquery.query({ query: sql });
        return { success: true, data: rows };
    } catch (err) {
        console.error("❌ SQL Error (Wave Monitoring):", err);
        throw err;
    }
}

// 🟢👆 จบส่วนที่ต้องวาง 👆🟢

app.listen(port, () => {
    console.log(`\n=================================================`);
    console.log(`🚀 Backend Server เปิดพร้อมทำงานแล้ว!`);
    console.log(`👉 รอรับคำสั่งอยู่ที่: http://localhost:${port}/api/run`);
    console.log(`=================================================\n`);
});
