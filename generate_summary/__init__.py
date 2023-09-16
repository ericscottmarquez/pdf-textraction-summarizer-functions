import os
from azure.functions import HttpRequest, HttpResponse
import openai

openai.api_type = "azure"
openai.api_base = "https://pdf-summarizer.openai.azure.com/"
openai.api_version = "2023-07-01-preview"
openai.api_key = os.getenv("OPENAI_API_KEY")

def main(req: HttpRequest) -> HttpResponse:
    pdf_name = req.params.get('pdf_name')
    summary_level = req.params.get('summary_level')
    
    if not pdf_name or not summary_level:
        return HttpResponse("Please provide both pdf_name and summary_level", status_code=400)
    
    # You would formulate your prompt here based on the pdf_name and summary_level
    prompt = f"Summarize the text file in your custom blob input data named '{pdf_name}' at a '{summary_level}' level of understanding."
    
    try:
        response = openai.ChatCompletion.create(
            engine="pdf-summarizer-model-deployment",
            messages=[
                {"role": "system", "content": "You are an AI assistant that helps people find information."},
                {"role": "user", "content": prompt}
            ],
            temperature=0,
            max_tokens=800,
            top_p=1,
            frequency_penalty=0,
            presence_penalty=0,
            stop=None
        )

        summary_text = response['choices'][0]['message']['content']
        
        # Save the response to a file
        version = "v0.1"
        file_path = os.path.join("path/to/your/folder", f"gpt-summary_{version}.txt")
        with open(file_path, 'w') as file:
            file.write(summary_text)
        
        # Stream the response back (here it's returned in one go, but you could modify to stream)
        return HttpResponse(summary_text, status_code=200)
    except Exception as e:
        return HttpResponse(str(e), status_code=500)
