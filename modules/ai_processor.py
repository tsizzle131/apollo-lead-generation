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
from rate_limiter import rate_limiter

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
    
    def generate_icebreaker(self, contact_info: Dict[str, Any], website_summaries: List[str]) -> Dict[str, str]:
        """
        Generate a personalized icebreaker AND subject line for a contact
        
        Args:
            contact_info: Contact information dictionary
            website_summaries: List of website page summaries
            
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
                return self._generate_b2b_icebreaker(contact_info, website_summaries)
            
            # Otherwise use normal personalized approach
            # Prepare contact profile
            first_name = contact_info.get('first_name', '')
            last_name = contact_info.get('last_name', '')
            headline = contact_info.get('headline', '')
            location = contact_info.get('location', '')
            company_name = contact_info.get('company_name', contact_info.get('company', ''))
            
            profile = f"{first_name} {last_name} {headline}"
            if company_name:
                profile += f" at {company_name}"
            
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
            
            # Enhanced prompt that requests both icebreaker and subject line
            enhanced_prompt = prompt_with_values + variation_instructions + "\n" + connection_style + """

ADDITIONALLY, create a compelling email subject line that:
1. Is 30-50 characters MAX (mobile-optimized)
2. Be DIRECT and create genuine curiosity
3. Avoid clickbait and marketing speak

Subject line approaches (pick what feels most natural):
- Question format: "Quick question about [Company]'s [specific thing]"
- Observation: "Noticed [Company]'s [specific approach/strategy]"
- Connection: "[Company] + [relevant solution/topic]?"
- Direct with name: "[Name], question about [specific area]"
- Specific reference (ONLY if highly relevant): Recent funding/news/expansion

BAD examples (avoid these):
- "[Company]'s edge in [industry]" (too vague)
- "Transform your [thing]" (sounds spammy)
- "Unlock growth potential" (generic marketing)

GOOD examples (aim for these):
- "Mike, quick question about GrowthLab's SEO"
- "Noticed GrowthLab's content strategy"
- "GrowthLab + scaling B2B outreach?"
- "Question about your SaaS clients"
- "Congrats on the Series B!" (only if they actually raised funding)

Return your response in this EXACT JSON format:
{
  "icebreaker": "your personalized icebreaker message",
  "subject_line": "your direct, curiosity-driven subject line (30-50 chars)"
}"""
            
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
            
            # Ensure subject line isn't too long (trim if needed)
            if len(subject_line) > 60:
                subject_line = subject_line[:57] + "..."
            
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
        """Create a fallback subject line with variety"""
        import random
        if company_name and len(company_name) > 3:
            # Truncate company name if needed
            short_company = company_name[:20] if len(company_name) > 20 else company_name
            return random.choice([
                f"Quick question about {short_company}",
                f"{first_name}, about {short_company[:15]}",
                f"{short_company} + automation?",
                f"Idea for {short_company}",
                f"{short_company} growth opportunity",
            ])
        else:
            return random.choice([
                f"Quick question, {first_name}",
                f"{first_name}, 30 seconds?",
                f"Idea for you, {first_name}",
                f"{first_name} - quick thought",
                f"Relevant for you, {first_name}",
            ])
    
    def _generate_b2b_icebreaker(self, contact_info: Dict[str, Any], website_summaries: List[str]) -> Dict[str, str]:
        """
        Generate a B2B icebreaker for business contacts (not individual decision makers)
        This is used when we have generic business emails like info@, contact@, etc.
        """
        try:
            # Reload config
            reload_config()
            from config import AI_MODEL_ICEBREAKER, AI_TEMPERATURE
            
            # Get business information
            business_name = contact_info.get('name') or contact_info.get('organization', {}).get('name', '')
            category = contact_info.get('organization', {}).get('category', '')
            website = contact_info.get('website_url', '')
            location = contact_info.get('organization', {}).get('city', '')
            
            website_content = "\n".join(website_summaries) if website_summaries else "No website content available"
            
            # B2B specific prompt
            b2b_prompt = f"""
You are writing to a BUSINESS EMAIL (info@, contact@, etc.), not a specific person.
The email should be appropriate for whoever handles business inquiries at this company.

Business: {business_name}
Type: {category}
Location: {location}
Website: {website}

Website Content Summary:
{website_content}

Create a professional B2B outreach email that:
1. Addresses the business/team (not a specific person)
2. Clearly states who we are and why we're reaching out
3. Mentions something specific about THEIR business (from website/category)
4. Asks to be directed to the right person (owner/manager/decision maker)
5. Is concise and professional

The email should feel like a legitimate business inquiry, not cold sales.

Good opening examples:
- "Hi {business_name} team,"
- "Hello,"
- "Good morning,"

Include a line like:
- "Could you direct me to the person who handles [relevant area]?"
- "I'd love to speak with whoever manages [relevant department]"
- "Could you connect me with the owner or decision maker regarding [topic]?"

Subject line should be:
- Professional and clear
- Reference the business name or type
- 30-50 characters

Return JSON format:
{{
  "icebreaker": "your B2B outreach message",
  "subject_line": "professional subject line"
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
            # Fallback B2B message
            return {
                "icebreaker": f"Hello {business_name} team,\n\nI came across your business and was impressed by what you're doing in {category or 'your industry'}.\n\nWe help businesses like yours [relevant value prop]. Could you direct me to the person who handles business development or partnerships?\n\nThank you for your time.",
                "subject_line": f"Partnership opportunity for {business_name[:20]}"
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