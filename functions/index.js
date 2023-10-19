const functions = require('firebase-functions');
const admin = require('firebase-admin');
const axios = require('axios');
const qs = require('qs');
const {
    Pool
} = require('pg');
const sql = require('sql');

admin.initializeApp();

// env
const DB_USER = process.env.DB_USER || "dope";
const DB_HOST = process.env.DB_HOST || "34.70.176.107";
const DB_DATABASE = process.env.DB_DATABASE || "postgres";
const DB_PASSWORD = process.env.DB_PASSWORD || "}+O(v\\fM7<vJ`?ve";
const X_API_KEY = process.env.X_API_KEY || "5zidIDqSy35C4rabpnzKJ7HD72tkJ7rISZ8BySTh";
const CLIENT_ID = process.env.CLIENT_ID || "partner.api.oakcity";
const CLIENT_SECRET = process.env.CLIENT_SECRET || "_Z_xs)wT^fT3";
const TENANT = process.env.TENANT || "NXT";

const pool = new Pool({
    user: DB_USER,
    host: DB_HOST,
    database: DB_DATABASE,
    password: DB_PASSWORD,
    port: 5432,
});

exports.sync = functions.https.onRequest(async (req, res) => {
    try {
        //Talents
        const token = await getToken();
        const countTalents = await getCountRow('test_talents');
        const talentIDs = await getIDs(token, 'talents', countTalents);
        const talentInfo = await getInfo(token, 'talents', talentIDs);
        await insertTalentData(JSON.parse(talentInfo));

        // Jobs
        const countJobs = await getCountRow('test_jobs');
        const jobsIDs = await getIDs(token, 'jobs', countJobs);
        const jobsInfo = await getInfo(token, 'jobs', jobsIDs);
        await insertJobData(JSON.parse(jobsInfo));

        //Placements
        const countPlacements = await getCountRow('test_placements');
        const placementsIDs = await getIDs(token, 'placements', countPlacements);
        const placementsInfo = await getInfo(token, 'placements', placementsIDs);
        await insertPlacementData(JSON.parse(placementsInfo));

        res.json({
            result: `sync : susscess.`
        });

    } catch (error) {
        console.error('Error sync:', error);
        res.json({
            result: `Error sync: `,
            error
        });

    }
});

async function getToken() {
    const data = qs.stringify({
        'grant_type': 'client_credentials',
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
        'scope': 'avionte.aero.compasintegrationservice'
    });

    const config = {
        method: 'post',
        url: 'https://api.avionte.com/authorize/token',
        headers: {
            'x-api-key': X_API_KEY,
            'Content-Type': 'application/x-www-form-urlencoded',
        },
        data,
    };

    try {
        const response = await axios.request(config);
        return response.data;
    } catch (error) {
        console.error('Error getting token:', error);
        throw error;
    }
}

async function getCountRow(table) {
    try {
        const client = await pool.connect();
        const result = await client.query(`SELECT * FROM bold_v2.${table}`);
        const rowCount = result.rowCount;
        client.release();
        console.log('rowCount: ', rowCount)
        return rowCount;
    } catch (error) {
        console.error('Error counting rows:', error);
        throw error;
    }
}

async function getIDs(token, type, countRow) {
    const config = {
        method: 'get',
        url: `https://api.avionte.com/front-office/v1/${type}/ids/${parseInt(countRow)/50}/50`,
        headers: {
            'Tenant': TENANT,
            'x-api-key': X_API_KEY,
            'Authorization': `Bearer ${token.access_token}`,
        },
    };

    try {
        const response = await axios.request(config);
        return JSON.stringify(response.data);
    } catch (error) {
        console.error(`Error getting ${type} IDs:`, error);
        throw error;
    }
}

async function getInfo(token, type, data) {
    const config = {
        method: 'post',
        url: `https://api.avionte.com/front-office/v1/${type}/multi-query`,
        headers: {
            'Tenant': TENANT,
            'x-api-key': X_API_KEY,
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${token.access_token}`,
        },
        data,
    };

    try {
        const response = await axios.request(config);
        return JSON.stringify(response.data);
    } catch (error) {
        console.error(`Error getting ${type} info:`, error);
        throw error;
    }
}

async function insertTalentData(data) {
    try {
        const client = await pool.connect();
        const convertData = data.map((talent) => ({
            // id: '',
            avionte_id: talent.id,
            first_name: talent.firstName,
            middle_name: talent.middleName,
            last_name: talent.lastName,
            home_phone: talent.homePhone,
            work_phone: talent.workPhone,
            mobile_phone: talent.mobilePhone,
            page_number: talent.pageNumber,
            email_address: talent.emailAddress,
            tax_id_number: talent.taxIdNumber,
            birthday: talent.birthday,
            gender: talent.gender,
            hire_date: talent.hireDate,
            resident_address: JSON.stringify(talent.residentAddress),
            mailing_address: JSON.stringify(talent.mailingAddress),
            payroll_address: JSON.stringify(talent.payrollAddress),
            addresses: JSON.stringify(talent.address),
            status: talent.status,
            filing_status: talent.filingStatus,
            federal_allowances: talent.federalAllowances,
            state_allowances: talent.stateAllowances,
            additional_federal_withholding: talent.additionalFederalWithholding,
            i_9_validated_date: talent.i9ValidatedDate,
            front_office_id: talent.frontOfficeId,
            latest_activity_date: talent.latestActivityDate,
            latest_activity_name: talent.latestActivityName,
            link: talent.link,
            race: talent.race,
            veteran_status: talent.veteranStatus,
            email_opt_out: talent.emailOptOut,
            is_archived: talent.isArchived,
            placement_status: talent.placementStatus,
            w_2_consent: talent.w2Consent,
            electronic_1095_c_consent: talent.electronic1095CConsent,
            referred_by: talent.referredBy,
            availability_date: talent.availabilityDate,
            status_id: talent.statusId,
            office_name: talent.officeName,
            office_division: talent.officeDivision,
            entered_by_user_id: talent.enteredByUserId,
            entered_by_user: talent.enteredByUser,
            representative_user_email: talent.representativeUserEmail,
            created_date: talent.createdDate,
            last_updated_date: talent.lastUpdatedDate,
            latest_work: talent.latestWork,
            last_contacted: talent.lastContacted,
            electronic_1099_consent: talent.electronic1099Consent,
            text_consent: talent.textConsent,
            talent_resume: talent.talentResume,
        }))

        let testtalents = sql.define({
            name: 'test_talents',
            schema: 'bold_v2',
            columns: [
                'avionte_id',
                'first_name',
                'middle_name',
                'last_name',
                'home_phone',
                'work_phone',
                'mobile_phone',
                'page_number',
                'email_address',
                'tax_id_number',
                'birthday',
                'gender',
                'hire_date',
                'resident_address',
                'mailing_address',
                'payroll_address',
                'addresses',
                'status',
                'filing_status',
                'federal_allowances',
                'state_allowances',
                'additional_federal_withholding',
                'i_9_validated_date',
                'front_office_id',
                'latest_activity_date',
                'latest_activity_name',
                'link',
                'race',
                'disability',
                'veteran_status',
                'email_opt_out',
                'is_archived',
                'placement_status',
                'representative_user',
                'w_2_consent',
                'electronic_1095_c_consent',
                'referred_by',
                'availability_date',
                'status_id',
                'office_name',
                'office_division',
                'entered_by_user_id',
                'entered_by_user',
                'representative_user_email',
                'created_date',
                'last_updated_date',
                'latest_work',
                'last_contacted',
                'flag',
                'electronic_1099_consent',
                'text_consent',
                'talent_resume',
            ]
        });


        try {

            let query = testtalents.insert(convertData).returning(testtalents.avionte_id).toQuery();
            let {
                rows
            } = await client.query(query);
            console.log(rows);
            client.release();
        } catch (e) {
            console.error(e);
        }

    } catch (error) {
        throw error;
    }
}

async function insertJobData(data) {
    try {
        const client = await pool.connect();
        const convertData = data.map((info) => ({
            // id: info.id,
            avionte_id: info.id,
            positions: info.positions,
            start_date: (info.startDate) ? info.startDate.split('T')[0] : null,
            end_date: (info.endDate) ? info.endDate.split('T')[0] : null,
            pay_rates: JSON.stringify(info.payRates),
            bill_rates: JSON.stringify(info.billRates),
            title: info.title,
            cost_center: info.costCenter,
            employee_type: info.employeeType,
            workers_compensation_class_code: info.workersCompensationClassCode,
            worker_comp_code: JSON.stringify(info.workerCompCode),
            company_id: info.companyId,
            branch_id: info.branchId,
            front_office_id: info.frontOfficeId,
            address_id: info.addressId,
            worksite_address: JSON.stringify(info.worksiteAddress),
            po_id: info.poId,
            company_name: info.companyName,
            link: info.link,
            contact_id: info.contactId,
            status_id: info.statusId,
            status: info.status,
            posted: info.posted,
            created_date: info.createdDate,
            order_type: info.orderType,
            order_type_id: info.orderTypeId,
            representative_users: JSON.stringify(info.representativeUsers),
            is_archived: info.isArchived,
            o_t_type: info.oT_Type,
            entered_by_user_id: info.enteredByUserId,
            entered_by_user: info.enteredByUser,
            sales_rep_user_id: info.salesRepUserId,
            sales_rep_user: info.salesRepUser,
            description: info.description,
            custom_job_details: JSON.stringify(info.customJobDetails),
            last_updated_date: info.lastUpdatedDate,
            latest_activity_date: info.lastestActivityDate,
            latest_activity_name: info.lastestActivityName,
            has_no_end_date: info.hasNoEndDate,
            pay_period: info.payPeriod,
            placed: info.placed,
            overtime_rule_id: info.overtimeRuleId,
            start_time_local: (info.startTimeLocal) ? info.startTimeLocal : null,
            end_time_local: (info.endTimeLocal) ? info.endTimeLocal : null,
            shift_schedule_days: JSON.stringify(info.shiftScheduleDays),
            offer: info.offer,
            pick_list: info.pickList,
            post_job_to_mobile_app: info.postJobToMobileApp,
            origin: info.origin,
            worksiteaddress_id: info.worksiteaddressId,
            owner_user_id: info.ownerUserId,
            created_at: (info.createdAt) ? info.created_at : null,
            updated_at: (info.updatedAt) ? info.updated_at : null,
            created_by_id: (info.createdById) ? info.createdById : null,
            updated_by_id: (info.updateById) ? info.updateById : null,
        }))

        let testjobs = sql.define({
            name: 'test_jobs',
            schema: 'bold_v2',
            columns: [
                'avionte_id',
                'positions',
                'start_date',
                'end_date',
                'pay_rates',
                'bill_rates',
                'title',
                'cost_center',
                'employee_type',
                'workers_compensation_class_code',
                'worker_comp_code',
                'company_id',
                'branch_id',
                'front_office_id',
                'address_id',
                'worksite_address',
                'po_id',
                'company_name',
                'link',
                'contact_id',
                'status_id',
                'status',
                'posted',
                'created_date',
                'order_type',
                'order_type_id',
                'representative_users',
                'is_archived',
                'o_t_type',
                'entered_by_user_id',
                'entered_by_user',
                'sales_rep_user_id',
                'sales_rep_user',
                'description',
                'custom_job_details',
                'last_updated_date',
                'latest_activity_date',
                'latest_activity_name',
                'has_no_end_date',
                'pay_period',
                'placed',
                'overtime_rule_id',
                'start_time_local',
                'end_time_local',
                'shift_schedule_days',
                'offer',
                'pick_list',
                'post_job_to_mobile_app',
                'origin',
                'worksiteaddress_id',
                'owner_user_id',
                'created_at',
                'updated_at',
                'created_by_id',
                'updated_by_id',
            ]
        });

        try {
            let query = testjobs.insert(convertData).returning(testjobs.avionte_id).toQuery();
            let {
                rows
            } = await client.query(query);
            console.log(rows);
            client.release();
        } catch (e) {
            console.error(e);
        }

    } catch (error) {
        throw error;
    }
}

async function insertPlacementData(data) {
    try {
        const client = await pool.connect();
        const convertData = data.map((info) => ({
            // id: info.id,
            avionte_id: info.id,
            talent_id: info.talentId,
            job_id: info.jobId,
            extension_id: info.extensionId,
            start_date: (info.startDate) ? info.startDate.split('T')[0] : null,
            end_date: (info.endDate) ? info.endDate.split('T')[0] : null,
            is_active: info.isActive,
            pay_rates: info.payRates,
            bill_rates: info.billRates,
            commission_users: info.commissionUsers,
            estimated_gross_profit: info.estimatedGrossProfit,
            employment_type: info.employmentType,
            front_office_id: info.frontOfficeId,
            is_permanent_placement_extension: info.isPermanentPlacementExtension,
            pay_basis: info.payBasis,
            hired_date: info.hiredDate,
            placement_additional_rates: info.placementAdditionalRates,
            end_reason_id: info.endReasonId,
            end_reason: info.endReason,
            entered_by_user_id: info.enteredByUserId,
            entered_by_user: info.enteredByUser,
            recruiter_user_id: info.recruiterUserId,
            recruiter_user: info.recruiterUser,
            custom_details: info.customDetails,
            created_by_user_id: info.createdByUserId,
            created_date: (info.createdDate) ? info.createdDate : null,
            has_no_end_date: info.hasNoEndDate,
            original_start_date: (info.originalStartDate) ? info.originalStartDate.split('T')[0] : null,
            final_end_date: (info.finalEndDate) ? info.finalEndDate.split('T')[0] : null,
            created_at: (info.createdAt) ? info.createdAt : null,
            updated_at: (info.updatedAt) ? info.updatedAt : null,
            created_by_id: (info.createdById) ? info.createdById : null,
            updated_by_id: (info.updatedById) ? info.updatedById : null,
        }))

        let testplacements = sql.define({
            name: 'test_placements',
            schema: 'bold_v2',
            columns: [
                // 'id',
                'avionte_id',
                'talent_id',
                'job_id',
                'extension_id',
                'start_date',
                'end_date',
                'is_active',
                'pay_rates',
                'bill_rates',
                'commission_users',
                'estimated_gross_profit',
                'employment_type',
                'front_office_id',
                'is_permanent_placement_extension',
                'pay_basis',
                'hired_date',
                'placement_additional_rates',
                'end_reason_id',
                'end_reason',
                'entered_by_user_id',
                'entered_by_user',
                'recruiter_user_id',
                'recruiter_user',
                'custom_details',
                'created_by_user_id',
                'created_date',
                'has_no_end_date',
                'original_start_date',
                'final_end_date',
                'created_at',
                'updated_at',
                'created_by_id',
                'updated_by_id',
            ]
        });

        try {
            let query = testplacements.insert(convertData).returning(testplacements.avionte_id).toQuery();
            let {
                rows
            } = await client.query(query);
            console.log(rows);
            client.release();
        } catch (e) {
            console.error(e);
        }

    } catch (error) {
        throw error;
    }
}