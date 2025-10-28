import logging
import time
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from openai import OpenAI
from config import (
    OPENAI_API_KEY, AI_MODEL_SUMMARY, AI_MODEL_ICEBREAKER,
    AI_TEMPERATURE, DELAY_BETWEEN_AI_CALLS, SUMMARY_PROMPT,
    ICEBREAKER_PROMPT, reload_config, MAX_AI_WORKERS
)
from .rate_limiter import rate_limiter

class AIProcessor:
    def __init__(self, api_key: str = None):
        # Always get the latest API key from UI config
        if not api_key:
            reload_config()
            from config import OPENAI_API_KEY
            api_key = OPENAI_API_KEY
        
        self.client = OpenAI(api_key=api_key)
        logging.info(f"🤖 AIProcessor initialized with API key: {api_key[:15] if api_key else 'None'}...")
        
    def summarize_website_pages(self, page_summaries: List[Dict[str, Any]]) -> List[str]:
        """
        Summarize multiple website pages using AI (now with parallel processing)
        
        Args:
            page_summaries: List of dictionaries with 'url' and 'content' keys
            
        Returns:
            List of summary strings
        """
        if not page_summaries:
            return []
        
        # Check if parallel processing is enabled
        from config import ENABLE_PARALLEL_PROCESSING
        if not ENABLE_PARALLEL_PROCESSING:
            # Fallback to sequential processing
            return self._summarize_pages_sequential(page_summaries)
        
        summaries = [None] * len(page_summaries)  # Pre-allocate list to maintain order
        
        with ThreadPoolExecutor(max_workers=min(MAX_AI_WORKERS, len(page_summaries))) as executor:
            # Submit all summarization tasks
            future_to_index = {}
            for i, page in enumerate(page_summaries):
                content = page.get('content', '')
                if not content or content.strip() == '<div>empty</div>':
                    summaries[i] = "no content"
                    continue
                
                future = executor.submit(self._generate_page_summary_with_rate_limit, content)
                future_to_index[future] = i
            
            # Collect results as they complete
            for future in as_completed(future_to_index):
                index = future_to_index[future]
                try:
                    summary = future.result()
                    summaries[index] = summary
                except Exception as e:
                    logging.error(f"Error summarizing page {page_summaries[index].get('url', 'unknown')}: {e}")
                    summaries[index] = "no content"
        
        return summaries
    
    def _summarize_pages_sequential(self, page_summaries: List[Dict[str, Any]]) -> List[str]:
        """Fallback sequential processing"""
        summaries = []
        for page in page_summaries:
            try:
                content = page.get('content', '')
                if not content or content.strip() == '<div>empty</div>':
                    summaries.append("no content")
                    continue
                
                summary = self._generate_page_summary(content)
                summaries.append(summary)
                
                # Use rate limiter instead of fixed delay
                reload_config()
                from config import AI_MODEL_SUMMARY
                rate_limiter.wait_for_openai(AI_MODEL_SUMMARY)
                
            except Exception as e:
                logging.error(f"Error summarizing page {page.get('url', 'unknown')}: {e}")
                summaries.append("no content")
        
        return summaries
    
    def _generate_page_summary_with_rate_limit(self, content: str) -> str:
        """Generate a summary with rate limiting"""
        # Wait for rate limit
        reload_config()
        from config import AI_MODEL_SUMMARY
        rate_limiter.wait_for_openai(AI_MODEL_SUMMARY)
        
        return self._generate_page_summary(content)
    
    def _generate_page_summary(self, content: str) -> str:
        """Generate a summary for a single page"""
        try:
            # Reload config to get latest prompts from UI
            reload_config()
            from config import SUMMARY_PROMPT, AI_MODEL_SUMMARY, AI_TEMPERATURE
            
            messages = [
                {
                    "role": "system",
                    "content": "You're a helpful, intelligent website scraping assistant. Always return responses in JSON format."
                },
                {
                    "role": "user", 
                    "content": SUMMARY_PROMPT
                },
                {
                    "role": "user",
                    "content": content
                }
            ]
            
            response = self.client.chat.completions.create(
                model=AI_MODEL_SUMMARY,
                messages=messages,
                temperature=AI_TEMPERATURE,
                response_format={"type": "json_object"}
            )
            
            result = response.choices[0].message.content
            
            # Parse JSON response
            import json
            parsed = json.loads(result)
            return parsed.get('abstract', 'no content')
            
        except Exception as e:
            logging.error(f"Error generating page summary: {e}")
            return "no content"
    
    def generate_icebreaker(self, contact_info: Dict[str, Any], website_summaries: List[str], organization_data: Dict[str, Any] = None) -> Dict[str, str]:
        """
        Generate a personalized icebreaker AND subject line for a contact

        Args:
            contact_info: Contact information dictionary
            website_summaries: List of website page summaries
            organization_data: Organization/product information (product_name, product_description, value_proposition, etc.)

        Returns:
            Dictionary with 'icebreaker' and 'subject_line' keys
        """
        try:
            import random
            # Reload config to get latest prompts and settings from UI
            reload_config()
            from config import ICEBREAKER_PROMPT, AI_MODEL_ICEBREAKER, AI_TEMPERATURE, ORGANIZATION_CONFIG
            
            # Check if this is a business contact (from local business scraper)
            is_business_contact = contact_info.get('is_business_contact', False)
            email = contact_info.get('email', '')
            email_status = contact_info.get('email_status', '')
            
            # Detect generic business emails
            generic_prefixes = ['info@', 'contact@', 'hello@', 'sales@', 'support@', 'admin@', 'office@']
            is_generic_email = any(email.lower().startswith(prefix) for prefix in generic_prefixes)
            
            # If it's a business contact or generic email, use B2B approach
            if is_business_contact or is_generic_email or email_status == 'business_email':
                return self._generate_b2b_icebreaker(contact_info, website_summaries, organization_data)
            
            # Otherwise use normal personalized approach
            # Prepare contact profile
            first_name = contact_info.get('first_name', '')
            last_name = contact_info.get('last_name', '')
            headline = contact_info.get('headline', '')
            location = contact_info.get('location', '')
            company_name = contact_info.get('company_name', contact_info.get('company', ''))

            # Build profile with null checks (Bug #7 fix)
            name_parts = [p for p in [first_name, last_name] if p]
            name = ' '.join(name_parts) if name_parts else 'there'
            if headline:
                profile = f"{name} {headline}"
            elif company_name:
                profile = f"{name} at {company_name}"
            else:
                profile = name
            
            # Handle empty website summaries gracefully
            if website_summaries and len(website_summaries) > 0:
                website_content = "\n".join(website_summaries)
            else:
                # No website data - focus on role and industry
                website_content = f"""No website content available. Create an icebreaker based on:
- Their role/title: {headline if headline else 'Not specified'}
- Company: {company_name if company_name else 'Not specified'}
- Industry context and common challenges in their field
- DO NOT mention that their website is blocked, protected, or unavailable
- Focus on industry-specific pain points or opportunities"""
            
            # Add variation instructions to reduce repetitive patterns
            variation_instructions = random.choice([
                "\n\nSTYLE: Start with a question about their business.",
                "\n\nSTYLE: Lead with an observation about their industry.",
                "\n\nSTYLE: Open with their name and a direct statement.",
                "\n\nSTYLE: Begin with an insight about their market.",
                "\n\nSTYLE: Start with what caught your attention.",
            ])
            
            connection_style = random.choice([
                "Make the connection to our solution subtle and natural.",
                "Be direct about how we can help.",
                "Focus on their pain point first, then our solution.",
                "Highlight a specific opportunity we can address.",
                "Connect through a shared challenge in their industry.",
            ])
            
            # Replace variables in the prompt with actual values
            prompt_with_values = ICEBREAKER_PROMPT
            
            # For local business flow, replace template variables
            if '{{company_name}}' in prompt_with_values:
                # This is the organization-specific prompt with variables
                business_name = contact_info.get('name') or contact_info.get('company_name', '')
                business_type = contact_info.get('organization', {}).get('category', '') or contact_info.get('category', 'business')
                location_city = contact_info.get('organization', {}).get('city', '') or contact_info.get('city', '')
                location_state = contact_info.get('organization', {}).get('state', '') or contact_info.get('state', '')
                location = f"{location_city}, {location_state}" if location_city else "your area"
                
                # Replace all template variables
                prompt_with_values = prompt_with_values.replace('{{company_name}}', business_name)
                prompt_with_values = prompt_with_values.replace('{{business_type}}', business_type)
                prompt_with_values = prompt_with_values.replace('{{location}}', location)
                prompt_with_values = prompt_with_values.replace('{{website_summaries}}', website_content)
            
            # Add random subject line style variation
            subject_line_styles = [
                "curiosity-gap", "value-driven", "specific-observation",
                "pattern-interrupt", "direct-benefit", "social-proof",
                "location-specific", "industry-insight", "unexpected-angle"
            ]
            chosen_style = random.choice(subject_line_styles)

            # Enhanced prompt that DEMANDS unique, high-converting subject lines
            enhanced_prompt = prompt_with_values + variation_instructions + "\n" + connection_style + f"""

CRITICAL: CREATE A UNIQUE, HIGH-CONVERTING EMAIL SUBJECT LINE

MANDATORY REQUIREMENTS:
1. Length: 25-45 characters (mobile-optimized)
2. Style for this email: {chosen_style.upper().replace('-', ' ')}
3. MUST be UNIQUE - NO GENERIC PATTERNS ALLOWED
4. Use SPECIFIC details from the business (location, category, rating, name)

❌ ABSOLUTELY FORBIDDEN PATTERNS (do NOT use these):
- "Inquiry for [X]" or "Inquiry about [X]"
- "Quick question about [X]"
- "Question about [X]"
- "Re: [X]"
- "Regarding [X]"
- "[Company]'s [thing]"
- Any variation of "quick question"
- Generic greetings like "Hello" or "Hi there"

✅ REQUIRED PERSONALIZATION - Pick ONE approach and execute it PERFECTLY:

1. LOCATION-SPECIFIC (if location available):
   - "[City] [category] owners listen up"
   - "Your [category] spot in [City]"
   - "[Neighborhood] [business type] idea"
   Example: "Brooklyn cafe owners listen up"

2. RATING/REPUTATION (if rating >= 4.0):
   - "Your [rating]★ secret?"
   - "[X] stars - here's how to 5"
   - "Top-rated [category] in [city]"
   Example: "Your 4.8★ secret?"

3. CATEGORY-SPECIFIC INSIGHT:
   - "[Category] revenue trick"
   - "Most [category]s miss this"
   - "[Category] automation FYI"
   Example: "Restaurant revenue trick"

4. PATTERN INTERRUPT:
   - "[Business name] → more [desired outcome]"
   - "re: your [category] biz"
   - "[Business] question from [your name]"
   Example: "Joe's Coffee → more walk-ins"

5. SOCIAL PROOF:
   - "17 [category]s use this"
   - "[City] [category]s switching to..."
   - "Your competitor just did this"
   Example: "17 dentists use this"

6. VALUE-SPECIFIC:
   - "3x more [outcome] for [category]"
   - "[Category] bookings system"
   - "Save [X] hours weekly"
   Example: "3x more orders for restaurants"

7. CURIOSITY WITH SPECIFICITY:
   - "What [X] [category]s know"
   - "[Business] missing out?"
   - "This helps busy [category] owners"
   Example: "What top cafes know"

8. UNEXPECTED ANGLE:
   - "Your [category] website issue"
   - "[Business] Google visibility"
   - "Noticed [Business]'s [specific thing]"
   Example: "Your restaurant's Google ranking"

COMPOSITION RULES:
- Use numbers when possible (3x, 17, 5 stars)
- Reference their specific business name OR category (not both)
- If they have location, use it creatively
- If they have high rating, reference it
- Be conversational, not corporate
- Create curiosity WITHOUT clickbait
- Test would YOU open this email?

EXAMPLES OF HIGH-CONVERTING SUBJECT LINES:
- "Brooklyn pizza spot opportunity" (location + category)
- "Your 4.9★ reviews → more sales" (rating + benefit)
- "Dental practice automation FYI" (category + value)
- "23 NYC cafes switched" (social proof + location)
- "Joe's Diner visibility issue" (name + specific problem)
- "Austin restaurant owners" (location + category)
- "Your competitor just did this" (competitive angle)
- "Bakery order system upgrade" (category + specific)

QUALITY CHECK - Your subject line MUST:
✓ Be 25-45 characters
✓ Use at LEAST ONE specific detail (name/location/category/rating)
✓ NOT use any forbidden patterns
✓ Create genuine curiosity
✓ Sound natural when read aloud
✓ Be different from "inquiry" or "question" patterns

Return your response in this EXACT JSON format:
{{
  "icebreaker": "your personalized icebreaker message",
  "subject_line": "your unique, high-converting subject line (25-45 chars)"
}}"""
            
            messages = [
                {
                    "role": "system",
                    "content": "You're a helpful, intelligent sales assistant. Always return responses in valid JSON format with both 'icebreaker' and 'subject_line' fields."
                },
                {
                    "role": "user",
                    "content": enhanced_prompt
                },
                {
                    "role": "assistant",
                    "content": """{"icebreaker":"Hey Aina,\\n\\nLove what you're doing at Maki. Also doing some outsourcing right now, wanted to run something by you.\\n\\nSo I hope you'll forgive me, but I creeped you/Maki quite a bit. I know that discretion is important to you guys (or at least I'm assuming this given the part on your website about white-labelling your services) and I put something together a few months ago that I think could help. To make a long story short, it's an outreach system that uses AI to find people hiring website devs. Then pitches them with templates (actually makes them a white-labelled demo website). Costs just a few cents to run, very high converting, and I think it's in line with Maki's emphasis on scalability.","subject_line":"Quick question about Maki's scaling"}"""
                },
                {
                    "role": "user",
                    "content": f"Profile: {profile}\n\nWebsite: {website_content}"
                }
            ]
            
            response = self.client.chat.completions.create(
                model=AI_MODEL_ICEBREAKER,
                messages=messages,
                temperature=AI_TEMPERATURE,
                response_format={"type": "json_object"}
            )
            
            result = response.choices[0].message.content
            
            # Parse JSON response with robust error handling
            import json
            try:
                parsed = json.loads(result)
            except json.JSONDecodeError as e:
                logging.error(f"Failed to parse AI response as JSON: {e}")
                logging.error(f"Raw response: {result}")
                # Fallback to basic parsing
                parsed = {"icebreaker": result, "subject_line": f"Quick question, {first_name}"}
            
            icebreaker = parsed.get('icebreaker', '').strip()
            subject_line = parsed.get('subject_line', '').strip()
            
            # Validate and potentially fix subject line
            if not subject_line:
                # Generate fallback subject if missing
                if company_name:
                    subject_line = f"Quick question about {company_name[:20]}"
                else:
                    subject_line = f"Quick question, {first_name}"
            
            # Ensure subject line isn't too long (trim if needed) - Bug #6 fix
            if len(subject_line) > 50:
                subject_line = subject_line[:47] + "..."
            
            # Validate icebreaker content
            if not icebreaker or len(icebreaker) < 20:
                logging.warning(f"AI returned empty/short icebreaker for {first_name} - creating fallback")
                fallback = self._create_basic_fallback(first_name, headline)
                if not subject_line:
                    subject_line = self._create_fallback_subject(first_name, company_name)
                return {"icebreaker": fallback, "subject_line": subject_line}
            
            logging.info(f"Generated icebreaker and subject for {first_name} {last_name}")
            logging.debug(f"Subject line ({len(subject_line)} chars): {subject_line}")
            return {"icebreaker": icebreaker, "subject_line": subject_line}
            
        except Exception as e:
            # Smart retry logic for rate limits and temporary errors
            return self._handle_ai_error(e, contact_info, website_summaries)
    
    def _handle_ai_error(self, error: Exception, contact_info: dict, website_summaries: list, attempt: int = 1) -> dict:
        """Handle AI API errors with smart retry logic"""
        import time
        error_str = str(error).lower()
        first_name = contact_info.get('first_name', 'unknown')
        
        # Rate limit error (429) - wait and retry
        if "rate" in error_str or "429" in error_str:
            if attempt <= 3:
                wait_time = 60 + (attempt * 20)  # 60s, 80s, 100s
                logging.warning(f"⏰ Rate limit hit for {first_name}, waiting {wait_time}s (attempt {attempt}/3)")
                time.sleep(wait_time)
                return self._retry_icebreaker_generation(contact_info, website_summaries, attempt + 1)
            else:
                logging.error(f"❌ Rate limit retries exhausted for {first_name}")
                return {"icebreaker": "Rate limit exceeded - could not generate icebreaker", "subject_line": f"Quick question, {first_name}"}
        
        # Server errors (500, 502, 503) - exponential backoff
        elif any(code in error_str for code in ["500", "502", "503", "server"]):
            if attempt <= 3:
                wait_time = 10 * (2 ** (attempt - 1))  # 10s, 20s, 40s
                logging.warning(f"🔧 Server error for {first_name}, retrying in {wait_time}s (attempt {attempt}/3)")
                time.sleep(wait_time)
                return self._retry_icebreaker_generation(contact_info, website_summaries, attempt + 1)
            else:
                logging.error(f"❌ Server error retries exhausted for {first_name}")
                return {"icebreaker": "Server error - could not generate icebreaker", "subject_line": f"Quick question, {first_name}"}
        
        # Timeout or network errors - quick retry
        elif any(term in error_str for term in ["timeout", "network", "connection"]):
            if attempt <= 2:
                wait_time = 5 * attempt  # 5s, 10s
                logging.warning(f"🌐 Network error for {first_name}, retrying in {wait_time}s (attempt {attempt}/2)")
                time.sleep(wait_time)
                return self._retry_icebreaker_generation(contact_info, website_summaries, attempt + 1)
            else:
                logging.error(f"❌ Network error retries exhausted for {first_name}")
                return {"icebreaker": "Network error - could not generate icebreaker", "subject_line": f"Quick question, {first_name}"}
        
        # Unknown error - create basic fallback icebreaker
        else:
            logging.error(f"❌ Unknown AI error for {first_name}: {error}")
            # Create a simple fallback based on contact info
            first_name = contact_info.get('first_name', 'there')
            headline = contact_info.get('headline', '')
            if headline:
                fallback = f"Hi {first_name},\n\nNoticed your work as {headline}. We're building something that might align with your expertise.\n\nInterested in a quick conversation?"
            else:
                fallback = f"Hi {first_name},\n\nCame across your profile and thought there might be some synergy with what we're working on.\n\nWould love to connect."
            return {"icebreaker": fallback, "subject_line": f"Quick question, {first_name}"}
    
    def _create_basic_fallback(self, first_name: str, headline: str) -> str:
        """Create a basic fallback icebreaker"""
        if headline:
            return f"Hi {first_name},\n\nNoticed your work as {headline}. Working on something that might be relevant to your expertise.\n\nWould love to connect and share what we're building."
        else:
            return f"Hi {first_name},\n\nCame across your profile and thought there might be some interesting synergy with what we're working on.\n\nWould you be open to a brief conversation?"
    
    def _create_fallback_subject(self, first_name: str, company_name: str = None) -> str:
        """Create a fallback subject line with variety - NO GENERIC PATTERNS"""
        import random
        if company_name and len(company_name) > 3:
            # Truncate company name if needed
            short_company = company_name[:20] if len(company_name) > 20 else company_name
            return random.choice([
                f"{short_company} → more customers",
                f"{short_company} automation FYI",
                f"Idea for {short_company}",
                f"{short_company} growth system",
                f"Your {short_company[:15]} biz",
                f"{short_company} competitive edge",
                f"{first_name} - {short_company[:12]} tip",
            ])
        else:
            return random.choice([
                f"{first_name}, 30 seconds?",
                f"Idea for you, {first_name}",
                f"{first_name} - quick tip",
                f"Relevant for you, {first_name}",
                f"{first_name}, saw your profile",
                f"{first_name} opportunity",
            ])
    
    def _generate_b2b_icebreaker(self, contact_info: Dict[str, Any], website_summaries: List[str], organization_data: Dict[str, Any] = None) -> Dict[str, str]:
        """
        Generate a B2B icebreaker for business contacts (not individual decision makers)
        This is used when we have generic business emails like info@, contact@, etc.

        Args:
            contact_info: Contact/business information
            website_summaries: Website content summaries
            organization_data: The sender's organization/product information
        """
        try:
            # Reload config
            reload_config()
            from config import AI_MODEL_ICEBREAKER, AI_TEMPERATURE
            
            # Get business information with rich context
            business_name = contact_info.get('name') or contact_info.get('organization', {}).get('name', '')
            category = contact_info.get('organization', {}).get('category', '') or contact_info.get('category', '')
            website = contact_info.get('website_url', '')
            city = contact_info.get('organization', {}).get('city', '') or contact_info.get('city', '')
            state = contact_info.get('organization', {}).get('state', '') or contact_info.get('state', '')
            rating = contact_info.get('organization', {}).get('rating') or contact_info.get('rating')
            reviews_count = contact_info.get('organization', {}).get('reviews_count') or contact_info.get('reviews_count')
            description = contact_info.get('organization', {}).get('description', '')

            # Format location nicely
            location = f"{city}, {state}" if city and state else city or state or "your area"

            # Format reputation info
            reputation_info = ""
            if rating and reviews_count:
                reputation_info = f"Rating: {rating}/5 stars from {reviews_count} reviews"
            elif rating:
                reputation_info = f"Rating: {rating}/5 stars"

            website_content = "\n".join(website_summaries) if website_summaries else "No specific website content available"

            # Extract organization product information
            product_info = ""
            company_name = ""
            if organization_data:
                company_name = organization_data.get('name', '')
                product_name = organization_data.get('product_name', '')
                product_description = organization_data.get('product_description', '')
                value_proposition = organization_data.get('value_proposition', '')
                target_audience = organization_data.get('target_audience', '')
                messaging_tone = organization_data.get('messaging_tone', 'professional')

                product_info = f"""
YOUR PRODUCT/SERVICE INFORMATION:
Company Name: {company_name if company_name else 'Not specified'}
Product/Service: {product_name if product_name else 'Not specified'}
Description: {product_description if product_description else 'Not specified'}
Value Proposition: {value_proposition if value_proposition else 'Not specified'}
Target Audience: {target_audience if target_audience else 'Not specified'}
Tone: {messaging_tone}

IMPORTANT:
- When mentioning your company, use: "{company_name if company_name else 'we'}"
- Use the above product information to craft your value proposition. Be specific about YOUR product/service and how it helps THEIR industry ({category}).
- Be conversational and natural - you can say "we" instead of repeating the company name multiple times.
"""

            # Enhanced B2B prompt - Generate COMPLETE email body
            b2b_prompt = f"""
You're reaching out to a LOCAL BUSINESS via their general business email (info@, contact@, hello@, etc.).

PROSPECT BUSINESS DETAILS:
Name: {business_name}
Type: {category}
Location: {location}
{reputation_info}
{f"Description: {description}" if description else ""}
Website: {website}

WEBSITE CONTENT (if available):
{website_content}

{product_info}

YOUR GOAL: Write a COMPLETE, personalized B2B email body (NOT just an opener). The user will only add their signature.

EMAIL STRUCTURE (5-7 sentences total):
1. **Personalized Opener** (1-2 sentences):
   - Reference specific details: location, rating, reviews, or website content
   - Show you actually researched them
   - Examples:
     * "Hey - saw you're running a {category} in {location}."
     * "Noticed {business_name}'s {rating}-star rating and {reviews_count} reviews."
     * "Caught your {category} business in {location} on Google Maps."

2. **Value Proposition** (2-3 sentences):
   - Use YOUR PRODUCT INFORMATION above to explain what you offer
   - Connect YOUR product/service to THEIR industry ({category}) specifically
   - Be SPECIFIC - use details from your product description and value proposition
   - Use plain language - no buzzwords
   - Make it relevant to {category} businesses in {location}

3. **Social Proof / Why Now** (1 sentence):
   - Reference their success if they have good ratings:
     * "With your {rating}-star rating, you're clearly doing something right."
   - OR mention industry-specific pain point:
     * "Most {category}s struggle with [specific problem]."

4. **Call-to-Action** (1 sentence):
   - Direct question or request
   - Ask to connect with owner/decision maker
   - Examples:
     * "Could you forward this to the owner or whoever handles [marketing/growth/partnerships]?"
     * "Would you be open to a quick 15-minute call?"
     * "Who's the best person to chat with about this?"

TONE:
- Conversational (like texting a colleague)
- Direct and honest (no fluff)
- Respectful (asking for permission, not demanding)
- Professional but NOT corporate

AVOID:
- "I came across your business" (too generic)
- "I was impressed by" (sounds fake)
- "Transform/Revolutionize/Unlock" (marketing BS)
- Long paragraphs (keep it skimmable)
- Vague claims without specifics

SUBJECT LINE REQUIREMENTS - CRITICAL:
Length: 25-40 characters max

❌ FORBIDDEN (do NOT use):
- "Quick Q for [X]"
- "Question about [X]"
- "Inquiry for [X]"
- Any "question" pattern

✅ REQUIRED - Use ONE of these approaches:
1. Location + Category: "{city} {category}s"
   Example: "Austin restaurant owners"

2. Category + Benefit: "{category} [specific outcome]"
   Example: "Dental practice automation"

3. Rating + Action: "Your {rating}★ reviews?"
   Example: "Your 4.8★ reviews?"

4. Social Proof: "[X] {category}s switched"
   Example: "23 cafes switched"

5. Pattern Interrupt: "[Business name] → more [outcome]"
   Example: "Joe's → more customers"

6. Problem-Specific: "{category} [specific issue]"
   Example: "Restaurant online orders"

Pick the MOST RELEVANT approach based on available data (location, rating, category).
Make it SPECIFIC and UNIQUE - NOT generic.

Return JSON format:
{{
  "icebreaker": "COMPLETE email body (5-7 sentences covering opener, value prop, social proof, CTA)",
  "subject_line": "unique, specific subject line (25-40 chars, NO 'question' patterns)"
}}
"""
            
            messages = [
                {
                    "role": "system",
                    "content": "You're a professional B2B outreach specialist. Generate business-appropriate emails for generic business email addresses."
                },
                {
                    "role": "user",
                    "content": b2b_prompt
                }
            ]
            
            response = self.client.chat.completions.create(
                model=AI_MODEL_ICEBREAKER,
                messages=messages,
                temperature=AI_TEMPERATURE,
                response_format={"type": "json_object"}
            )
            
            result = response.choices[0].message.content
            
            # Parse JSON response
            import json
            parsed = json.loads(result)
            
            # Wait for rate limit
            rate_limiter.wait_for_openai(AI_MODEL_ICEBREAKER)
            
            return parsed
            
        except Exception as e:
            logging.error(f"Error generating B2B icebreaker: {e}")
            # Fallback B2B message - COMPLETE email body
            # Safely extract variables (they may not be defined if error occurred early)
            safe_business_name = contact_info.get('name', 'your business')
            safe_category = contact_info.get('category', 'business')
            safe_city = contact_info.get('city', '')
            safe_location = contact_info.get('organization', {}).get('city', '') or safe_city
            safe_rating = contact_info.get('rating')

            location_str = f" in {safe_location}" if safe_location else ""
            rating_str = f" with a {safe_rating}-star rating" if safe_rating else ""

            fallback_email = f"""Hey - noticed {safe_business_name} is a {safe_category}{location_str}{rating_str}.

We help local {safe_category} [your value proposition here - be specific to their industry].

[Add 1-2 sentences about what you do and how it helps {safe_category} businesses specifically.]

Could you forward this to the owner or whoever handles new partnerships?

Thanks!"""

            # Use random fallback subject instead of forbidden "Quick Q" pattern
            import random
            fallback_subjects = [
                f"{safe_business_name[:20]} → more customers",
                f"{safe_city} {safe_category[:15]}" if safe_city and safe_category else f"{safe_category[:20]} tip",
                f"{safe_business_name[:15]} opportunity",
                f"{safe_category[:20]} automation FYI" if safe_category else f"{safe_business_name[:20]} idea",
                f"Idea for {safe_business_name[:18]}",
            ]
            return {
                "icebreaker": fallback_email,
                "subject_line": random.choice(fallback_subjects)
            }
    
    def _retry_icebreaker_generation(self, contact_info: dict, website_summaries: list, attempt: int) -> dict:
        """Retry icebreaker generation with the same parameters"""
        try:
            # Reload config to get latest prompts and settings from UI
            from config import reload_config
            reload_config()
            from config import ICEBREAKER_PROMPT, AI_MODEL_ICEBREAKER, AI_TEMPERATURE
            
            # Prepare contact profile
            first_name = contact_info.get('first_name', '')
            last_name = contact_info.get('last_name', '')
            headline = contact_info.get('headline', '')
            location = contact_info.get('location', '')
            
            profile = f"{first_name} {last_name} {headline}"
            website_content = "\n".join(website_summaries)
            
            messages = [
                {
                    "role": "system",
                    "content": "You're a helpful, intelligent sales assistant. Always return responses in JSON format."
                },
                {
                    "role": "user",
                    "content": ICEBREAKER_PROMPT
                },
                {
                    "role": "assistant",
                    "content": """{"icebreaker":"Hey Aina,\\n\\nLove what you're doing at Maki. Also doing some outsourcing right now, wanted to run something by you.\\n\\nSo I hope you'll forgive me, but I creeped you/Maki quite a bit. I know that discretion is important to you guys (or at least I'm assuming this given the part on your website about white-labelling your services) and I put something together a few months ago that I think could help. To make a long story short, it's an outreach system that uses AI to find people hiring website devs. Then pitches them with templates (actually makes them a white-labelled demo website). Costs just a few cents to run, very high converting, and I think it's in line with Maki's emphasis on scalability."}"""
                },
                {
                    "role": "user",
                    "content": f"Profile: {profile}\n\nWebsite: {website_content}"
                }
            ]
            
            response = self.client.chat.completions.create(
                model=AI_MODEL_ICEBREAKER,
                messages=messages,
                temperature=AI_TEMPERATURE,
                response_format={"type": "json_object"}
            )
            
            result = response.choices[0].message.content
            
            # Parse JSON response
            import json
            parsed = json.loads(result)
            icebreaker = parsed.get('icebreaker', '')
            
            logging.info(f"✅ Retry successful for {first_name} {last_name} (attempt {attempt})")
            return {"icebreaker": icebreaker}
            
        except Exception as retry_error:
            # Recursive call to handle the retry error
            return self._handle_ai_error(retry_error, contact_info, website_summaries, attempt)
    
    def test_connection(self) -> bool:
        """Test if OpenAI API connection is working"""
        try:
            # Test with a simple API call
            response = self.client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[{"role": "user", "content": "Hello"}],
                max_tokens=5
            )
            
            if response.choices:
                logging.info("OpenAI API connection successful")
                return True
            else:
                logging.error("OpenAI API test failed: No response")
                return False
                
        except Exception as e:
            logging.error(f"OpenAI API test error: {e}")
            return False
    
    def batch_process_contacts(self, contacts_with_summaries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Process multiple contacts in batch, generating icebreakers for each
        
        Args:
            contacts_with_summaries: List of contact dictionaries with website summaries
            
        Returns:
            List of contacts with added icebreakers
        """
        processed_contacts = []
        
        for contact in contacts_with_summaries:
            try:
                website_summaries = contact.get('website_summaries', [])
                icebreaker = self.generate_icebreaker(contact, website_summaries)
                
                # Add icebreaker to contact data
                contact['mutiline_icebreaker'] = icebreaker
                processed_contacts.append(contact)
                
                # Rate limiting between contacts
                time.sleep(DELAY_BETWEEN_AI_CALLS)
                
            except Exception as e:
                logging.error(f"Error processing contact {contact.get('first_name', 'unknown')}: {e}")
                # Add contact without icebreaker
                contact['mutiline_icebreaker'] = "Error generating icebreaker"
                processed_contacts.append(contact)
        
        return processed_contacts