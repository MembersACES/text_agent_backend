"""
Base 1 Review - OpenAI Chat-based Extraction
Handles conversational extraction using OpenAI API
"""
import os
import json
import logging
from typing import List, Dict, Optional
from openai import OpenAI
import base64

logger = logging.getLogger(__name__)

# Initialize OpenAI client
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
if not OPENAI_API_KEY:
    logger.warning("OPENAI_API_KEY not set - OpenAI features will not work")

client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None


def extract_pdf_text(file_path: str) -> str:
    """Extract text from PDF file"""
    try:
        import PyPDF2
        with open(file_path, 'rb') as file:
            pdf_reader = PyPDF2.PdfReader(file)
            text = ""
            for page in pdf_reader.pages:
                text += page.extract_text() + "\n"
            return text
    except Exception as e:
        logger.error(f"Error extracting PDF text: {e}")
        return ""


def chat_with_openai(messages: List[Dict], pdf_text: Optional[str] = None, run_context: Optional[Dict] = None) -> str:
    """Send messages to OpenAI and get response"""
    if not client:
        return "OpenAI API key not configured. Please set OPENAI_API_KEY environment variable."
    
    try:
        # Prepare messages
        chat_messages = messages.copy()
        
        # Build system message with Base 1 Review context
        system_content = """You are a utility cost analyst performing Base 1 Reviews - preliminary assessments of business utility costs (electricity, gas, water, waste, cleaning) based solely on invoice data.

## Your Role
You help businesses understand their utility costs and identify savings opportunities by:
1. Extracting comprehensive invoice data (rates, usage, charges)
2. Comparing costs against market benchmarks
3. Identifying low-hanging fruit (easy wins for cost reduction)
4. Answering questions about invoices and accounts
5. Helping refine and correct extracted data

## Base 1 Review Process
When a user uploads invoices or asks for a review:
- Extract all relevant data (customer details, billing periods, usage, rates, charges)
- Identify accounts by NMI (electricity) or MRIN (gas)
- Compare rates against market benchmarks
- Highlight cost savings opportunities
- Provide clear, actionable insights

## Market Rate Benchmarks (Australia - Victoria)
**Electricity - SME:** Peak 25-32 c/kWh, Off-peak 18-24 c/kWh, Supply $1.20-$1.80/day
**Electricity - C&I:** Peak 20-28 c/kWh, Off-peak 15-22 c/kWh, Supply $1.50-$5.00/day
**Gas - SME:** ~$18.44/GJ unbundled, Supply $0.90-$1.20/day
**Gas - C&I:** ~$16.75/GJ unbundled, Supply $1.00-$1.50/day

Be helpful, accurate, and focus on actionable insights for cost savings."""
        
        # If PDF text is provided, add it as context
        if pdf_text:
            system_content += f"""

The user has uploaded an invoice. Here is the extracted text from the PDF:

{pdf_text[:8000]}

IMPORTANT: 
- For electricity invoices, identify the NMI (National Meter Identifier) - usually 10-11 alphanumeric characters
- For gas invoices, identify the MRIN (Meter Register Identification Number) - usually 8-12 alphanumeric characters
- NMI/MRIN are critical for grouping invoices by account/site"""
        
        # Add run context if available (accounts, documents)
        if run_context:
            accounts = run_context.get("accounts", {})
            if accounts:
                system_content += "\n\nCurrent accounts identified:\n"
                for acc_id, acc_data in accounts.items():
                    nmi_mrin = acc_data.get("nmi") or acc_data.get("mrin") or acc_data.get("account_number", "Unknown")
                    system_content += f"- {nmi_mrin}: {acc_data.get('utility_type', 'Unknown')} ({len(acc_data.get('doc_ids', []))} invoice(s))\n"
        
        system_message = {
            "role": "system",
            "content": system_content
        }
        chat_messages.insert(0, system_message)
        
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=chat_messages,
            temperature=0.3,
            max_tokens=2000
        )
        
        return response.choices[0].message.content.strip()
    
    except Exception as e:
        logger.error(f"OpenAI API error: {e}")
        return f"Error communicating with AI: {str(e)}"


def extract_invoice_summary(pdf_text: str) -> Dict:
    """Get comprehensive invoice extraction using OpenAI based on Base 1 Review requirements"""
    if not client:
        return {"error": "OpenAI API key not configured"}
    
    try:
        # STEP 1: First, identify what type of utility this is
        utility_type = identify_utility_type_quick(pdf_text)
        
        # STEP 2: Use specialized extraction based on type
        if "electricity" in utility_type.lower():
            extracted = extract_electricity_specialized(pdf_text)
        elif "gas" in utility_type.lower():
            extracted = extract_gas_specialized(pdf_text)
        else:
            extracted = extract_generic_utility(pdf_text)
        
        # STEP 3: Clean up the numbers
        extracted = clean_numeric_fields(extracted)
        
        # STEP 4: Add opportunities analysis
        extracted["low_hanging_fruit"] = identify_low_hanging_fruit(extracted)
        
        return extracted
    
    except Exception as e:
        logger.error(f"Error extracting invoice summary: {e}")
        return {"error": str(e)}


def identify_utility_type_quick(pdf_text: str) -> str:
    """Quick identification of utility type"""
    if not client:
        return "Unknown"
    
    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": "You identify utility invoice types. Respond with ONLY one word: Electricity, Gas, Water, Waste, or Other."
                },
                {
                    "role": "user",
                    "content": f"First 1000 characters of invoice:\n{pdf_text[:1000]}"
                }
            ],
            temperature=0,
            max_tokens=10
        )
        return response.choices[0].message.content.strip()
    except:
        return "Unknown"


def extract_electricity_specialized(pdf_text: str) -> Dict:
    """Specialized extraction just for electricity invoices"""
    
    prompt = """Extract data from this electricity invoice. Return ONLY a valid JSON object.

Rules:
1. ALL numbers must be actual numbers, not text or math expressions
2. If you see "100 + 200", calculate it and return 300
3. Remove $ signs and commas from numbers
4. Use null for missing fields, 0 for missing numbers

Required JSON format:
{
  "business_name": "company name",
  "supplier": "energy retailer",
  "nmi": "10-11 digit NMI code",
  "site_address": "service address",
  "invoice_date": "DD/MM/YYYY",
  "invoice_number": "invoice number",
  "billing_period_start": "DD/MM/YYYY",
  "billing_period_end": "DD/MM/YYYY",
  "billing_days": 30,
  "peak_usage_kwh": 1500.5,
  "off_peak_usage_kwh": 800.2,
  "shoulder_usage_kwh": 0,
  "total_usage_kwh": 2300.7,
  "peak_rate_c_per_kwh": 28.5,
  "off_peak_rate_c_per_kwh": 22.3,
  "daily_supply_charge": 1.45,
  "total_inc_gst": 856.50,
  "utility_type": "Electricity"
}"""

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "You extract data from invoices. Return ONLY valid JSON, no markdown, no explanation."},
            {"role": "user", "content": f"{prompt}\n\nInvoice text:\n{pdf_text[:8000]}"}
        ],
        temperature=0.1,
        max_tokens=1500
    )
    
    content = response.choices[0].message.content.strip()
    
    # Remove markdown if present
    if "```json" in content:
        content = content.split("```json")[1].split("```")[0].strip()
    elif "```" in content:
        content = content.split("```")[1].split("```")[0].strip()
    
    # Remove any text before { or after }
    start = content.find('{')
    end = content.rfind('}')
    if start != -1 and end != -1:
        content = content[start:end+1]
    
    return json.loads(content)


def extract_gas_specialized(pdf_text: str) -> Dict:
    """Specialized extraction just for gas invoices"""
    
    prompt = """Extract data from this gas invoice. Return ONLY a valid JSON object.

Rules:
1. ALL numbers must be actual numbers, not text
2. Remove $ signs and commas
3. Use null for missing fields, 0 for missing numbers

Required JSON format:
{
  "business_name": "company name",
  "supplier": "gas retailer",
  "mrin": "8-12 digit MRIN code",
  "site_address": "service address",
  "invoice_date": "DD/MM/YYYY",
  "invoice_number": "invoice number",
  "billing_period_start": "DD/MM/YYYY",
  "billing_period_end": "DD/MM/YYYY",
  "billing_days": 30,
  "total_usage_mj": 15000,
  "total_usage_gj": 15,
  "daily_supply_charge": 1.10,
  "total_inc_gst": 450.50,
  "utility_type": "Gas"
}"""

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "You extract data from invoices. Return ONLY valid JSON."},
            {"role": "user", "content": f"{prompt}\n\nInvoice text:\n{pdf_text[:8000]}"}
        ],
        temperature=0.1,
        max_tokens=1500
    )
    
    content = response.choices[0].message.content.strip()
    
    # Clean up
    if "```json" in content:
        content = content.split("```json")[1].split("```")[0].strip()
    elif "```" in content:
        content = content.split("```")[1].split("```")[0].strip()
    
    start = content.find('{')
    end = content.rfind('}')
    if start != -1 and end != -1:
        content = content[start:end+1]
    
    return json.loads(content)


def extract_generic_utility(pdf_text: str) -> Dict:
    """Generic extraction for other utilities"""
    
    prompt = """Extract basic data from this utility invoice. Return ONLY valid JSON.

{
  "business_name": "company name",
  "supplier": "supplier name",
  "site_address": "service address",
  "account_number": "account number",
  "invoice_date": "DD/MM/YYYY",
  "invoice_number": "invoice number",
  "total_inc_gst": 250.00,
  "utility_type": "Water/Waste/Other"
}"""

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "You extract data from invoices. Return ONLY valid JSON."},
            {"role": "user", "content": f"{prompt}\n\nInvoice text:\n{pdf_text[:8000]}"}
        ],
        temperature=0.1,
        max_tokens=800
    )
    
    content = response.choices[0].message.content.strip()
    
    if "```json" in content:
        content = content.split("```json")[1].split("```")[0].strip()
    elif "```" in content:
        content = content.split("```")[1].split("```")[0].strip()
    
    start = content.find('{')
    end = content.rfind('}')
    if start != -1 and end != -1:
        content = content[start:end+1]
    
    return json.loads(content)


def clean_numeric_fields(extracted: Dict) -> Dict:
    """Clean all numeric fields to ensure they're actual numbers"""
    
    numeric_fields = [
        'total_inc_gst', 'peak_usage_kwh', 'off_peak_usage_kwh', 
        'shoulder_usage_kwh', 'total_usage_kwh', 'peak_rate_c_per_kwh',
        'off_peak_rate_c_per_kwh', 'daily_supply_charge', 'billing_days',
        'demand_kw', 'meter_charges', 'total_usage_mj', 'total_usage_gj'
    ]
    
    for field in numeric_fields:
        if field in extracted and extracted[field] is not None:
            extracted[field] = safe_clean_number(extracted[field])
    
    return extracted


def safe_clean_number(value) -> float:
    """Convert any value to a clean float"""
    if value is None or value == "":
        return 0.0
    
    # Already a number
    if isinstance(value, (int, float)):
        return float(value)
    
    # It's a string - clean it
    value_str = str(value).strip()
    
    # Remove currency and formatting
    value_str = value_str.replace('$', '').replace(',', '').replace(' ', '')
    
    # If it has math operators, try to calculate
    if any(op in value_str for op in ['+', '-', '*', '/']):
        try:
            # Only evaluate if it looks safe (just numbers and operators)
            import re
            if re.match(r'^[0-9+\-*/.() ]+$', value_str):
                return float(eval(value_str))
        except:
            pass
    
    # Simple conversion
    try:
        return float(value_str)
    except:
        return 0.0


def identify_low_hanging_fruit(extracted: Dict) -> List[Dict]:
    """Identify cost savings opportunities (low-hanging fruit)"""
    opportunities = []
    
    utility_type = extracted.get("utility_type", "").lower()
    
    if utility_type == "electricity":
        # Check peak rate
        peak_rate = extracted.get("peak_rate_c_per_kwh")
        if peak_rate:
            try:
                peak_rate_val = float(str(peak_rate).replace("$", "").replace(",", ""))
                # SME benchmark: 25-32 c/kWh, C&I: 20-28 c/kWh
                if peak_rate_val > 32:
                    opportunities.append({
                        "type": "high_peak_rate",
                        "severity": "high",
                        "message": f"Peak rate of {peak_rate_val:.2f} c/kWh is above market benchmark (25-32 c/kWh for SME, 20-28 c/kWh for C&I)",
                        "potential_savings": "Significant - consider competitive tender"
                    })
                elif peak_rate_val > 28:
                    opportunities.append({
                        "type": "elevated_peak_rate",
                        "severity": "medium",
                        "message": f"Peak rate of {peak_rate_val:.2f} c/kWh is at the higher end of market range",
                        "potential_savings": "Moderate - worth reviewing"
                    })
            except (ValueError, TypeError):
                pass
        
        # Check daily supply charge
        daily_supply = extracted.get("daily_supply_charge")
        if daily_supply:
            try:
                supply_val = float(str(daily_supply).replace("$", "").replace(",", ""))
                # SME benchmark: $1.20-$1.80/day, C&I: $1.50-$5.00/day
                if supply_val > 2.0:
                    opportunities.append({
                        "type": "high_supply_charge",
                        "severity": "medium",
                        "message": f"Daily supply charge of ${supply_val:.2f}/day may be above market rates",
                        "potential_savings": "Moderate"
                    })
            except (ValueError, TypeError):
                pass
        
        # Check off-peak rate
        off_peak_rate = extracted.get("off_peak_rate_c_per_kwh")
        if off_peak_rate:
            try:
                off_peak_val = float(str(off_peak_rate).replace("$", "").replace(",", ""))
                # Benchmark: 18-24 c/kWh for SME, 15-22 c/kWh for C&I
                if off_peak_val > 24:
                    opportunities.append({
                        "type": "high_off_peak_rate",
                        "severity": "medium",
                        "message": f"Off-peak rate of {off_peak_val:.2f} c/kWh is above market benchmark",
                        "potential_savings": "Moderate"
                    })
            except (ValueError, TypeError):
                pass
        
        # Check metering charges
        metering_charges = extracted.get("meter_charges")
        if metering_charges:
            try:
                metering_val = float(str(metering_charges).replace("$", "").replace(",", "").replace("+", ""))
                # Market benchmark: $75-100/month per meter, or $700-900/year
                annual_metering = metering_val * 12  # Assuming monthly
                if annual_metering > 1200:  # Above $100/month
                    opportunities.append({
                        "type": "high_metering_charges",
                        "severity": "high",
                        "message": f"Metering charges of ${metering_val:.2f}/month (${annual_metering:.2f}/year) are above market benchmark ($700-900/year)",
                        "potential_savings": "Significant - metering charges can often be reduced"
                    })
                elif annual_metering > 900:
                    opportunities.append({
                        "type": "elevated_metering_charges",
                        "severity": "medium",
                        "message": f"Metering charges of ${metering_val:.2f}/month (${annual_metering:.2f}/year) are at the higher end of market range",
                        "potential_savings": "Moderate"
                    })
            except (ValueError, TypeError):
                pass
        
        # Check demand charges
        demand_charges = extracted.get("demand_charges")
        demand_kw = extracted.get("demand_kw")
        if demand_charges and demand_kw:
            try:
                demand_charges_val = float(str(demand_charges).replace("$", "").replace(",", "").replace("+", ""))
                demand_kw_val = float(str(demand_kw).replace(",", ""))
                if demand_kw_val > 0:
                    # Calculate rate per kVA/month
                    monthly_rate = demand_charges_val / demand_kw_val
                    # Market benchmark: $10-15/kVA/month
                    if monthly_rate > 18:
                        opportunities.append({
                            "type": "high_demand_charges",
                            "severity": "high",
                            "message": f"Demand charges of ${demand_charges_val:.2f} for {demand_kw_val:.1f} kW (${monthly_rate:.2f}/kW/month) are above market benchmark ($10-15/kVA/month)",
                            "potential_savings": "Significant - demand charges are a major cost driver"
                        })
                    elif monthly_rate > 15:
                        opportunities.append({
                            "type": "elevated_demand_charges",
                            "severity": "medium",
                            "message": f"Demand charges of ${demand_charges_val:.2f} for {demand_kw_val:.1f} kW (${monthly_rate:.2f}/kW/month) are at the higher end of market range",
                            "potential_savings": "Moderate"
                        })
            except (ValueError, TypeError):
                pass
    
    elif utility_type == "gas":
        # Check usage rate (for unbundled, benchmark is ~$18.44/GJ for SME, ~$16.75/GJ for C&I)
        usage_rate = extracted.get("first_tier_rate") or extracted.get("second_tier_rate")
        total_usage_gj = extracted.get("total_usage_gj")
        if usage_rate and total_usage_gj:
            try:
                rate_val = float(str(usage_rate).replace("$", "").replace(",", ""))
                usage_val = float(str(total_usage_gj).replace(",", ""))
                # Convert to $/GJ if needed
                if "mj" in str(usage_rate).lower() or rate_val > 10:
                    # Likely in c/MJ, convert to $/GJ (divide by 10)
                    rate_per_gj = rate_val / 10
                else:
                    rate_per_gj = rate_val
                
                if rate_per_gj > 20:
                    opportunities.append({
                        "type": "high_gas_rate",
                        "severity": "high",
                        "message": f"Gas rate of ${rate_per_gj:.2f}/GJ is above market benchmark (~$18.44/GJ for SME, ~$16.75/GJ for C&I)",
                        "potential_savings": "Significant"
                    })
            except (ValueError, TypeError):
                pass
        
        # Check daily supply charge
        daily_supply = extracted.get("daily_supply_charge")
        if daily_supply:
            try:
                supply_val = float(str(daily_supply).replace("$", "").replace(",", ""))
                # Benchmark: $0.90-$1.20/day for SME, $1.00-$1.50/day for C&I
                if supply_val > 1.50:
                    opportunities.append({
                        "type": "high_gas_supply_charge",
                        "severity": "medium",
                        "message": f"Daily supply charge of ${supply_val:.2f}/day may be above market rates",
                        "potential_savings": "Moderate"
                    })
            except (ValueError, TypeError):
                pass
    
    # Check for high total charges (relative indicator)
    total_inc_gst = extracted.get("total_inc_gst")
    total_usage = extracted.get("total_usage_kwh") or extracted.get("total_usage_mj") or extracted.get("total_usage_gj")
    if total_inc_gst and total_usage:
        try:
            total_val = float(str(total_inc_gst).replace("$", "").replace(",", "").replace(" ", ""))
            usage_val = float(str(total_usage).replace(",", "").replace(" ", ""))
            if usage_val > 0:
                cost_per_unit = total_val / usage_val
                if utility_type == "electricity" and cost_per_unit > 0.35:  # $0.35 per kWh is very high
                    opportunities.append({
                        "type": "high_overall_cost",
                        "severity": "high",
                        "message": f"Overall cost of ${cost_per_unit:.3f} per kWh is very high - indicates potential for significant savings",
                        "potential_savings": "Significant - comprehensive review recommended"
                    })
        except (ValueError, TypeError):
            pass
    
    return opportunities

