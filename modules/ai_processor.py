import logging
import time
from typing import List, Dict, Any, Optional
from openai import OpenAI
from config import (
    OPENAI_API_KEY, AI_MODEL_SUMMARY, AI_MODEL_ICEBREAKER, 
    AI_TEMPERATURE, DELAY_BETWEEN_AI_CALLS, SUMMARY_PROMPT, 
    ICEBREAKER_PROMPT, reload_config
)

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
        Summarize multiple website pages using AI
        
        Args:
            page_summaries: List of dictionaries with 'url' and 'content' keys
            
        Returns:
            List of summary strings
        """
        summaries = []
        
        for page in page_summaries:
            try:
                content = page.get('content', '')
                if not content or content.strip() == '<div>empty</div>':
                    summaries.append("no content")
                    continue
                
                summary = self._generate_page_summary(content)
                summaries.append(summary)
                
                # Rate limiting between AI calls - get latest delay from UI
                reload_config()
                from config import DELAY_BETWEEN_AI_CALLS
                time.sleep(DELAY_BETWEEN_AI_CALLS)
                
            except Exception as e:
                logging.error(f"Error summarizing page {page.get('url', 'unknown')}: {e}")
                summaries.append("no content")
                
        return summaries
    
    def _generate_page_summary(self, content: str) -> str:
        """Generate a summary for a single page"""
        try:
            # Reload config to get latest prompts from UI
            reload_config()
            from config import SUMMARY_PROMPT, AI_MODEL_SUMMARY, AI_TEMPERATURE
            
            messages = [
                {
                    "role": "system",
                    "content": "You're a helpful, intelligent website scraping assistant."
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
    
    def generate_icebreaker(self, contact_info: Dict[str, Any], website_summaries: List[str]) -> str:
        """
        Generate a personalized icebreaker for a contact
        
        Args:
            contact_info: Contact information dictionary
            website_summaries: List of website page summaries
            
        Returns:
            Generated icebreaker string
        """
        try:
            # Reload config to get latest prompts and settings from UI
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
                    "content": "You're a helpful, intelligent sales assistant."
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
            
            logging.info(f"Generated icebreaker for {first_name} {last_name}")
            return {"icebreaker": icebreaker}
            
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
                return {"icebreaker": "Rate limit exceeded - could not generate icebreaker"}
        
        # Server errors (500, 502, 503) - exponential backoff
        elif any(code in error_str for code in ["500", "502", "503", "server"]):
            if attempt <= 3:
                wait_time = 10 * (2 ** (attempt - 1))  # 10s, 20s, 40s
                logging.warning(f"🔧 Server error for {first_name}, retrying in {wait_time}s (attempt {attempt}/3)")
                time.sleep(wait_time)
                return self._retry_icebreaker_generation(contact_info, website_summaries, attempt + 1)
            else:
                logging.error(f"❌ Server error retries exhausted for {first_name}")
                return {"icebreaker": "Server error - could not generate icebreaker"}
        
        # Timeout or network errors - quick retry
        elif any(term in error_str for term in ["timeout", "network", "connection"]):
            if attempt <= 2:
                wait_time = 5 * attempt  # 5s, 10s
                logging.warning(f"🌐 Network error for {first_name}, retrying in {wait_time}s (attempt {attempt}/2)")
                time.sleep(wait_time)
                return self._retry_icebreaker_generation(contact_info, website_summaries, attempt + 1)
            else:
                logging.error(f"❌ Network error retries exhausted for {first_name}")
                return {"icebreaker": "Network error - could not generate icebreaker"}
        
        # Unknown error - log and continue
        else:
            logging.error(f"❌ Unknown AI error for {first_name}: {error}")
            return {"icebreaker": "Error generating icebreaker"}
    
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
                    "content": "You're a helpful, intelligent sales assistant."
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