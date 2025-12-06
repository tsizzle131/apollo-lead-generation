import logging
import time
import hashlib
from enum import Enum
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from openai import OpenAI


class IcebreakerVariant(Enum):
    """Variants for A/B testing icebreaker prompts."""
    CONTROL = "control"              # Legacy approach (for comparison)
    PROSPECT_CENTRIC = "prospect"    # New 80/20 prospect-first approach (default)


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
    
    def generate_icebreaker(self, contact_info: Dict[str, Any], website_summaries: List[str], organization_data: Dict[str, Any] = None, template: str = None) -> Dict[str, str]:
        """
        Generate a personalized icebreaker AND subject line for a contact

        Args:
            contact_info: Contact information dictionary
            website_summaries: List of website page summaries
            organization_data: Organization/product information (product_name, product_description, value_proposition, etc.)
            template: Icebreaker template to use (specific_question, peer_social_proof, website_insight,
                      problem_agitation, curiosity_hook, direct_value, or 'auto' for weighted random)

        Returns:
            Dictionary with 'icebreaker', 'subject_line', 'template_used', and 'formula_used' keys
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
                return self._generate_b2b_icebreaker(contact_info, website_summaries, organization_data, template)
            
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
            # RESEARCH: 40 chars max for mobile visibility (33 chars shows on most devices)
            if len(subject_line) > 40:
                subject_line = subject_line[:37] + "..."
            
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

    def _infer_pain_points(self, category: str, rating: float = None, reviews_count: int = None) -> str:
        """Generate likely pain points based on business characteristics."""

        # Category-specific pain points
        CATEGORY_PAIN_MAP = {
            "restaurant": [
                "Managing online ordering across multiple platforms",
                "No-shows eating into revenue",
                "Staff turnover and training costs"
            ],
            "salon": [
                "Last-minute cancellations",
                "Retail product margins vs service time",
                "Client retention between appointments"
            ],
            "spa": [
                "Filling appointment gaps during slow periods",
                "Upselling retail products to service clients",
                "Competing with at-home wellness trends"
            ],
            "gym": [
                "Member churn after January rush",
                "Equipment maintenance costs",
                "Class scheduling optimization"
            ],
            "yoga": [
                "Class fill rates during off-peak hours",
                "Teacher retention and scheduling",
                "Competing with online yoga platforms"
            ],
            "chiropractor": [
                "Patient retention after initial treatment",
                "Insurance reimbursement delays",
                "Converting one-time visits to care plans"
            ],
            "dentist": [
                "Recall appointment no-shows",
                "Patient anxiety management",
                "Case acceptance for elective procedures"
            ],
            "wellness": [
                "Service bundling complexity",
                "Practitioner scheduling conflicts",
                "Client education on complementary services"
            ],
            "health_food": [
                "Competing with big-box retailers on price",
                "Educating customers on new products",
                "Managing inventory for niche products"
            ],
            "supplement": [
                "Standing out among hundreds of similar products",
                "Building customer trust in product quality",
                "Competing with online retailers"
            ],
            "pharmacy": [
                "Differentiation from chain pharmacies",
                "Building front-end retail sales",
                "Customer loyalty programs"
            ],
            "physical_therapy": [
                "Patient compliance with home exercises",
                "Insurance authorization delays",
                "Converting acute care to wellness programs"
            ],
            "massage": [
                "Filling weekday appointment slots",
                "Retail product recommendations",
                "Client retention and rebooking"
            ],
            "acupuncture": [
                "Educating patients on treatment benefits",
                "Building recurring treatment plans",
                "Competing with conventional medicine"
            ]
        }

        pain_points = []

        # Get category-specific pains
        if category:
            category_lower = category.lower().replace('_', ' ').replace('-', ' ')
            for key, pains in CATEGORY_PAIN_MAP.items():
                if key in category_lower or category_lower in key:
                    pain_points.extend(pains[:2])
                    break

        # Add size/reputation-based insights
        if rating and rating < 4.0:
            pain_points.append("Managing customer expectations and reviews")
        elif rating and rating >= 4.5 and reviews_count and reviews_count > 100:
            pain_points.append("Scaling while maintaining quality")

        if reviews_count and reviews_count < 50:
            pain_points.append("Building visibility and word-of-mouth")

        if not pain_points:
            pain_points = ["Standing out in a competitive local market", "Converting foot traffic to loyal customers"]

        return "\n".join([f"- {p}" for p in pain_points[:3]])

    def _is_perfect_fit(self, prospect_category: str, target_categories: list) -> bool:
        """Check if prospect's category matches product's target categories."""
        if not target_categories or not prospect_category:
            return False

        prospect_cat_lower = prospect_category.lower()
        for target in target_categories:
            target_lower = target.lower()
            if target_lower in prospect_cat_lower or prospect_cat_lower in target_lower:
                return True
        return False

    def _assign_variant(self, business_id: str, campaign_id: str = None) -> str:
        """
        Deterministic variant assignment based on business ID.
        Uses hash to ensure same business always gets same variant.

        Args:
            business_id: Unique business identifier
            campaign_id: Optional campaign ID for campaign-specific variants

        Returns:
            Variant name string (e.g., 'control', 'prospect')
        """
        # Create hash input from business + campaign for deterministic assignment
        hash_input = f"{business_id}:{campaign_id or 'default'}"
        hash_value = int(hashlib.md5(hash_input.encode()).hexdigest(), 16)

        # For now, 100% go to prospect-centric (the new approach)
        # Can adjust percentages here for A/B testing:
        # - 50/50 split: variants = [CONTROL, PROSPECT_CENTRIC], use hash_value % 2
        # - 100% new: always return PROSPECT_CENTRIC

        # Currently rolling out 100% to new approach
        # To enable A/B testing, uncomment below:
        # variants = [IcebreakerVariant.CONTROL, IcebreakerVariant.PROSPECT_CENTRIC]
        # variant_index = hash_value % len(variants)
        # return variants[variant_index].value

        return IcebreakerVariant.PROSPECT_CENTRIC.value

    def _get_formula_instructions(
        self,
        formula: str,
        business_name: str,
        category: str,
        city: str,
        rating: float,
        reviews_count: int,
        has_website_content: bool,
        is_perfect_fit: bool,
        product_description: str,
        value_proposition: str
    ) -> str:
        """
        Generate specific instructions based on the chosen icebreaker formula.

        RESEARCH-BACKED TEMPLATES (2024-2025 data):
        - Subject lines: 36-50 chars optimal, 33 chars for mobile visibility
        - Personalization: +133% reply rate increase
        - Question CTAs: 371% better than multiple CTAs
        - Single CTA with <6 words performs best

        Sources: Belkins 2025, Backlinko, Lemlist, Instantly benchmarks
        """

        # Build rating context if available
        rating_context = ""
        if rating and rating >= 4.5 and reviews_count and reviews_count > 50:
            rating_context = f"\n- USE their strong reviews ({reviews_count} reviews, {rating} stars) as a compliment"
        elif rating and reviews_count:
            rating_context = f"\n- Can reference their {reviews_count} reviews or {rating}-star rating"

        if formula == "WEBSITE_INSIGHT":
            # RESEARCH: Website reference emails get 8-12% reply rate (Belkins)
            return f"""
APPROACH: Lead with something SPECIFIC you found on their website.
- Pull out a unique detail: specific service, team member, specialty, technology they use
- Show you actually looked (not just scraped data)
- Connect to a genuine observation

AVAILABLE DATA TO USE:{rating_context}
- Business: {business_name}
- Category: {category}
- Location: {city}

EXAMPLE OPENERS (pick ONE - vary the style):
- "Saw on your site you specialize in [specific service]. Got something that pairs well with that."
- "Your [specific page/service] caught my attention. Working on something that could help."
- "The way you approach [thing from website] stood out. Quick question about that..."

CTA (pick one, question format, under 6 words):
- "Worth a look?"
- "Curious?"
- "Interested?"

DO: Reference something specific from their actual website content
DON'T: Default to review counts - save that for variety
DON'T: Start with "I noticed" or "I saw" (overused)
"""

        elif formula == "LOCAL_CONTEXT":
            # RESEARCH: Local/geographic personalization increases relevance
            return f"""
APPROACH: Reference their local market naturally - but DON'T start with "A few..."
- Mention {city} in a unique way that shows you know the area
- Position as someone who understands their specific market
- Make it feel local and specific, not templated

AVAILABLE DATA TO USE:{rating_context}
- Business: {business_name}
- Location: {city}

EXAMPLE OPENERS (pick ONE - DO NOT use "A few..." pattern):
- "Working with {city} {category}s on something. Your approach stood out."
- "{city}'s {category} scene is competitive. Got an edge that might help."
- "Know the {city} market well - thought of {business_name} when building this."
- "Seeing {city} {category}s focus more on [X] lately. You too?"

CTA (pick one, question format, under 6 words):
- "Relevant for you?"
- "Worth exploring?"
- "Sound useful?"

DO: Be specific about {city} - make it feel personal
DON'T: Start with "A few {city}..." - that's the SOCIAL_PROOF formula
"""

        elif formula == "INDUSTRY_QUESTION":
            # RESEARCH: Question-led subject lines get 46% open rate (Belkins)
            return f"""
APPROACH: Ask a genuine question about their business.
- Lead with curiosity, not a pitch
- Ask something they'll want to answer
- Make it specific to {category}

AVAILABLE DATA TO USE:{rating_context}
- Category: {category}
- Location: {city}

EXAMPLE OPENER:
"Do your patients ask for something to use between visits? Most {category}s I talk to get that question."

CTA (keep it simple):
- End with your question as the CTA
- Don't add a second ask

DO: Ask something relevant to their daily work
DON'T: Ask leading questions that feel salesy
"""

        elif formula == "SOCIAL_PROOF":
            # RESEARCH: Peer reference emails get 5-8% reply rate
            return f"""
APPROACH: Reference what similar businesses are doing - VARY the opening style.
- Mention other {category}s (don't name competitors)
- Be specific about what they're doing (not results you delivered)
- Let them draw the connection

AVAILABLE DATA TO USE:{rating_context}
- Category: {category}
- Location: {city}

EXAMPLE OPENERS (pick ONE - vary the style each time):
- "A few {city} {category}s started [doing X] this year. Patients love it."
- "Other {category}s in your area are trying [X]. Getting good results."
- "Talked to {category}s in {city} recently - they mentioned [problem]. You seeing that too?"
- "Similar practices to yours have been asking about [X]. Thought you might be interested."

CTA (pick one, question format, under 6 words):
- "Worth a look?"
- "On your radar?"
- "Interested?"

DO: Be genuine about what others are doing
DON'T: Promise specific results or make claims you can't verify
"""

        elif formula == "DIRECT_VALUE":
            # RESEARCH: Direct value gets 4-7% reply rate when highly relevant
            return f"""
APPROACH: State the value clearly and directly.
- Lead with what they get, not what you do
- Be specific to {category}
- Keep it confident but not pushy

AVAILABLE DATA TO USE:{rating_context}
- Category: {category}
- Value prop context: {value_proposition[:100] if value_proposition else 'Not specified'}

EXAMPLE OPENER:
"Helps {category}s [specific outcome]. Takes 2 minutes to see if it fits."

CTA (pick one, question format, under 6 words):
- "Worth 2 minutes?"
- "Interested?"
- "Want to see it?"

DO: Be confident about the value
DON'T: Promise to send catalogs, data, or materials (our AI handles replies)
"""

        elif formula == "CURIOSITY_HOOK":
            # RESEARCH: Curiosity-based emails get 6-10% reply rate
            return f"""
APPROACH: Open with an intriguing observation.
- Share something genuinely interesting about {category}s
- Create curiosity without clickbait
- Make them want to reply to learn more

AVAILABLE DATA TO USE:{rating_context}
- Category: {category}
- Location: {city}

EXAMPLE OPENER:
"Noticed something about {city} {category}s lately. You seeing [trend/pattern] too?"

CTA (pick one, question format, under 6 words):
- "Curious?"
- "Seeing this too?"
- "Worth discussing?"

DO: Be genuinely interesting
DON'T: Use fake urgency or clickbait
"""

        elif formula == "PROBLEM_AGITATION":
            # RESEARCH: PAS framework gets 5-9% reply rate
            # FIXED: Don't infer names from business names
            return f"""
APPROACH: Name a specific pain point they'll recognize.
- Identify a REAL challenge common to {category}s
- Briefly mention what it costs them (time, patients, revenue)
- Hint you have something that helps

CATEGORY-SPECIFIC PAIN POINTS:
- Chiropractor: Patients feel great after adjustment but relief fades before next visit
- Dentist: Recall no-shows, case acceptance
- Salon: Last-minute cancellations, rebooking
- Restaurant: No-shows, platform fees
- Physical therapy: Patient compliance between visits
- General: Customer retention, follow-up gaps

AVAILABLE DATA TO USE:{rating_context}
- Category: {category}
- Location: {city}

EXAMPLE OPENER (DON'T use "Dr." unless you have their actual name):
"One thing {category}s tell me: patients feel great leaving, but the relief fades before their next visit. Working on something that helps with that."

CTA (pick one, question format, under 6 words):
- "Worth exploring?"
- "Relevant for you?"
- "Open to a look?"

DO: Be specific about the problem
DON'T: Infer "Dr. [Name]" from business names - use generic greeting or skip the name
DON'T: Promise to send data, catalogs, or samples in the email
"""

        else:
            # Fallback
            return f"""
APPROACH: Keep it simple and conversational.
- Make it specific to {category}
- Show genuine interest
- End with a simple question

CTA: One question, under 6 words
"""

    def _generate_b2b_icebreaker(self, contact_info: Dict[str, Any], website_summaries: List[str], organization_data: Dict[str, Any] = None, template: str = None) -> Dict[str, str]:
        """
        Generate a B2B icebreaker for business contacts using varied approaches.

        Uses 6 research-backed templates to achieve 4-8%+ reply rates.
        Templates based on cold email studies (Belkins, Woodpecker, Backlinko, Gong).

        Args:
            contact_info: Contact/business information
            website_summaries: Website content summaries
            organization_data: The sender's organization/product information
            template: Optional template override ('auto', 'specific_question', 'peer_social_proof',
                      'website_insight', 'problem_agitation', 'curiosity_hook', 'direct_value')
        """
        import random

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

            website_content = "\n".join(website_summaries) if website_summaries else ""
            has_website_content = bool(website_content and website_content.strip())

            # Extract organization and product information
            company_name = ""
            product_name = ""
            product_description = ""
            value_proposition = ""
            target_categories = []

            if organization_data:
                company_name = organization_data.get('name', '')
                product_name = organization_data.get('product_name', '')
                product_description = organization_data.get('product_description', '')
                value_proposition = organization_data.get('value_proposition', '')
                target_categories = organization_data.get('target_categories', [])

            # Determine if this is a perfect-fit prospect
            is_perfect_fit = self._is_perfect_fit(category, target_categories)

            # 6 RESEARCH-BACKED ICEBREAKER TEMPLATES
            # Based on cold email studies (Belkins, Woodpecker, Backlinko, Gong)
            # Each targets 4-8%+ reply rates vs 1-3% average

            # Template to formula mapping
            TEMPLATE_TO_FORMULA = {
                'specific_question': 'INDUSTRY_QUESTION',   # 6-10% reply rate (Backlinko)
                'peer_social_proof': 'SOCIAL_PROOF',        # 5-8% reply rate (Single Grain)
                'website_insight': 'WEBSITE_INSIGHT',       # 8-12% reply rate (Belkins)
                'problem_agitation': 'PROBLEM_AGITATION',   # 5-9% reply rate (PAS framework)
                'curiosity_hook': 'CURIOSITY_HOOK',         # 6-10% reply rate (Belkins)
                'direct_value': 'DIRECT_VALUE',             # 4-7% reply rate (Authority principle)
            }

            formulas = [
                "WEBSITE_INSIGHT",      # Lead with specific website detail - 8-12% reply rate
                "LOCAL_CONTEXT",        # Reference their city/neighborhood
                "INDUSTRY_QUESTION",    # Ask genuine question - 6-10% reply rate
                "SOCIAL_PROOF",         # Reference similar businesses - 5-8% reply rate
                "DIRECT_VALUE",         # Lead with specific benefit - 4-7% reply rate
                "CURIOSITY_HOOK",       # Pattern interrupt opener - 6-10% reply rate
                "PROBLEM_AGITATION",    # Name their pain point - 5-9% reply rate
            ]

            # Select formula based on template or weighted random
            if template and template != 'auto' and template in TEMPLATE_TO_FORMULA:
                chosen_formula = TEMPLATE_TO_FORMULA[template]
                template_used = template
            else:
                # Weight formulas based on available data (auto mode)
                weights = [
                    3.0 if has_website_content else 0.5,  # WEBSITE_INSIGHT
                    2.0 if city else 1.0,                  # LOCAL_CONTEXT
                    2.0,                                   # INDUSTRY_QUESTION
                    1.5,                                   # SOCIAL_PROOF
                    2.0 if is_perfect_fit else 1.0,       # DIRECT_VALUE
                    1.5,                                   # CURIOSITY_HOOK
                    1.5,                                   # PROBLEM_AGITATION
                ]
                chosen_formula = random.choices(formulas, weights=weights, k=1)[0]
                template_used = 'auto'

            # Subject line style - randomly select to ensure variety
            subject_styles = [
                ("BUSINESS_NAME", f'Use "{business_name}" in the subject'),
                ("CITY_CATEGORY", f'Use "{city} {category}" format'),
                ("QUESTION", 'Ask a short question'),
                ("RE_STYLE", f'Use "re: {business_name[:15]}" style (like a reply)'),
                ("DIRECT", 'State the benefit directly'),
                ("CURIOSITY", 'Create curiosity about something specific'),
            ]
            chosen_subject_style, subject_instruction = random.choice(subject_styles)

            # Build formula-specific instructions
            formula_instructions = self._get_formula_instructions(
                chosen_formula,
                business_name,
                category,
                city,
                rating,
                reviews_count,
                has_website_content,
                is_perfect_fit,
                product_description,
                value_proposition
            )

            # RESEARCH-BACKED B2B Prompt (2024-2025 data)
            # Sources: Belkins, Backlinko, Lemlist, Instantly benchmarks
            # Key findings:
            # - 36-50 char subject lines optimal, 33 chars for mobile
            # - Question CTAs 371% better than multiple CTAs
            # - Personalization +133% reply rate
            # - Single CTA under 6 words performs best

            b2b_prompt = f"""
Write a cold email that sounds like a real person wrote it. Goal: Get a reply.

============================================
THEIR BUSINESS (personalize with this)
============================================
Business: {business_name}
Type: {category}
Location: {city}, {state}
Rating: {rating}/5 ({reviews_count} reviews)

Website insights:
{website_content if has_website_content else "No website content - use their category and location instead"}

============================================
YOUR APPROACH FOR THIS EMAIL: {chosen_formula}
============================================
{formula_instructions}

============================================
WHAT YOU'RE OFFERING (for context only)
============================================
Product: {product_description if product_description else 'Not specified'}
Value: {value_proposition if value_proposition else 'Not specified'}
Perfect fit: {'Yes - be confident' if is_perfect_fit else 'Maybe - ask first'}

============================================
WRITING RULES (critical for conversions)
============================================

**TONE:** Write like you're texting a business owner you respect but haven't met.
- Short sentences. Casual punctuation.
- No corporate speak. No buzzwords.
- Sound like a person, not a company.

**LENGTH:** 3-4 sentences MAX. Under 60 words total.
- Busy people delete long emails without reading
- If you can cut a word, cut it

**STRUCTURE:**
- Line 1: Hook them with something specific to THEM
- Line 2: Connect it to what you do (briefly)
- Line 3: Simple question CTA (under 6 words)

**ABSOLUTELY FORBIDDEN (instant spam folder):**
- "Quick question" - spam trigger
- "Hope this finds you well" - AI tell
- "reaching out" or "wanted to connect" - salesy
- "crushing it" - fake flattery
- Starting with "I noticed" or "I saw" - overused
- "businesses like yours" - too vague
- Anything over 4 sentences
- Using "Dr. [Name]" unless you have their ACTUAL name (not from business name)
- Promising to send materials, data, catalogs, or samples

**CTA RULES (CRITICAL - research shows this matters most):**
- ONE question only, under 6 words
- Must be a question (ends with ?)
- Low commitment: "Worth a look?" "Curious?" "Interested?"
- DO NOT offer to send anything specific
- DO NOT mention calls, demos, or meetings in first email

**OPENER VARIETY (CRITICAL - pick ONE, DO NOT always use reviews):**

IMPORTANT: Vary your opening style. If using reviews/ratings, limit to 30% of the time.

Style A - Question Hook (best for engagement):
"Do your patients ask for something to use between visits?"
"Ever wonder why [competitor] gets more walk-ins?"

Style B - Local Trend (builds credibility):
"A few {city} {category}s started [doing X] this year..."
"Noticed a trend among {city} {category}s lately..."

Style C - Problem Lead (shows understanding):
"One thing {category}s tell me: [specific pain point]..."
"The biggest challenge I hear from {category} owners..."

Style D - Website/Research Insight (when you have content):
"Saw on your site that you [specific thing]..."
"Your [specific service/page] caught my eye..."

Style E - Direct Value (for perfect fits):
"[Product] helps {category}s [specific outcome]..."
"Quick way for {category}s to [benefit]..."

Style F - Social Proof (sparingly):
"[X] reviews at [rating] stars - clearly doing something right..."
"Your [rating]-star rating stood out..."

OPENER RULES:
- NEVER start the same way twice in a batch
- Style F (reviews) should be used sparingly
- Styles A, B, C are highest-performing - use most often
- Match the opener to the chosen FORMULA above

============================================
SUBJECT LINE - MAX 40 CHARACTERS
============================================
STYLE: {chosen_subject_style}
INSTRUCTION: {subject_instruction}

**HARD REQUIREMENTS:**
- MAXIMUM 40 characters (mobile visibility)
- Optimal: 25-35 characters
- NO "Quick Q", "Quick question", or "Inquiry"
- Create curiosity without clickbait

**EXAMPLES BY STYLE:**
- BUSINESS_NAME: "saw {business_name[:12]}" (under 20 chars)
- CITY_CATEGORY: "{city} {category[:8]}s" (location + category)
- QUESTION: "between visits?" (short question)
- RE_STYLE: "re: your practice" (looks like reply)
- DIRECT: "patient take-home" (benefit focused)
- CURIOSITY: "{category[:10]} trend" (industry hook)

Return valid JSON:
{{
  "icebreaker": "your 3-4 sentence email (under 60 words, ending with question CTA)",
  "subject_line": "25-40 characters MAX"
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

            # Include which template was used for A/B tracking
            parsed['template_used'] = template_used
            parsed['formula_used'] = chosen_formula

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
                "subject_line": random.choice(fallback_subjects),
                "template_used": "fallback",
                "formula_used": "fallback"
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